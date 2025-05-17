module WorkShaper
  # The Manager is responsible for organizing the work to be done, triggering calls to acknowledge work done
  # for each offset in monotonically increasing order (independent of the execution order), and gracefully
  # cleaning up when `#shutdown` is called.
  class Manager
    # Several of the parameters here are Lambdas (not Proc). Note you can pass a method using
    # `method(:some_method)` or a lambda directly `->{ puts 'Hello'}`.
    #
    # @param work [#call(message, partition, offset)] Lambda that we will call to execute work.
    # @param on_done [#call(message, partition, offset)] Lambda that we call when work is done.
    # @param ack [] Lambda we will call when it is safe to commit an offset. This is not the
    #   same as Done.
    # @param on_error [#call(exception, message, partition, offset)] Lambda that we call if an
    #   error is encountered.
    # @param max_in_queue [Integer] The maximum in flight jobs per Sub Key. This affects how many
    #   message could get replayed if your process crashes before the offsets are committed.
    def initialize(work:, on_done:, ack:, on_error:, max_in_queue: 3,
                   heartbeat_period_sec: 60, offset_commit_period_ms: 5)
      @work = work
      @on_done = on_done
      @ack = ack
      @on_error = on_error
      @workers = {}
      @last_ack = {}
      @received_offsets = {}
      @completed_offsets = {}
      @max_in_queue = max_in_queue
      @semaphore = Mutex.new
      @shutdown = false

      @total_enqueued = 0

      @partition_failures = Hash.new { |h, k| h[k] = 0 }
      @max_partition_failures = ENV.fetch('WORKSHAPER_MAX_PARTITION_FAILURES', 5).to_i
      @partition_backoff_time = ENV.fetch('WORKSHAPER_PARTITION_BACKOFF_TIME', 30).to_i

      @heartbeat = Thread.new do
        while true
          report
          sleep heartbeat_period_sec
        end
      rescue => e
        WorkShaper.logger.warn({ message: 'Shutdown from Heartbeat', error: e })
        shutdown
      end

      @offset_manager = Thread.new do
        while true
          @completed_offsets.each_key do |partition|
            offset_ack(partition)
          end
          sleep offset_commit_period_ms / 1000.0
        end
      rescue => e
        WorkShaper.logger.warn({ message: 'Shutdown from Offset Manager', error: e })
        shutdown
      end
    end

    # Enqueue a message to be worked on the given `sub_key`, `partition`, and `offset`.
    def enqueue(sub_key, message, partition, offset)
      raise StandardError, 'Shutting down' if @shutdown

      if partition_blocked?(partition)
        WorkShaper.logger.warn({
          message: 'Partition blocked',
          partition: partition,
          failures: @partition_failures[partition],
        })
      end

      pause_on_overrun

      worker = nil
      @semaphore.synchronize do
        @total_enqueued += 1
        (@received_offsets[partition] ||= SortedSet.new) << offset

        worker =
          @workers[sub_key] ||=
            Worker.new(
              @work,
              @on_done,
              method(:offset_ack),
              @on_error,
              @last_ack,
              @completed_offsets,
              @semaphore,
              @max_in_queue
            )
      end

      worker.enqueue(message, partition, offset)
    rescue => e
      @partition_failures[partition] += 1
      if @partition_failures[partition] >= @max_partition_failures
        block_partition(partition)
      end
      raise e
    end

    # Flush any offsets for which work has been completed. Only lowest continuous run of
    # offsets will be acknowledged. Any offset after a discontinuity will be replayed when
    # the consumer restarts.
    def flush(safe: true)
      sleep 5
      @completed_offsets.each_key do |k|
        safe ? offset_ack(k) : offset_ack_unsafe(k)
      end
    end

    # Output state of Last Acked and Pending Offset Ack's.
    def report(detailed: false)
      @semaphore.synchronize do
        WorkShaper.logger.info({
          message: 'Reporting',
          total_enqueued: @total_enqueued,
          total_acked: @total_acked,
          in_flight: (@total_enqueued.to_i - @total_acked.to_i),
          last_acked_offsets: @last_ack,
          worker_count: @workers.keys.count,
          partition_stats: gather_partition_stats
        })
        if detailed
          WorkShaper.logger.info(
            {
              messaage: 'Reporting - Extra Detail',
              pending_ack: @completed_offsets,
              received_offsets: @received_offsets
            })
        end
      end
    end

    # Stop the underlying threads
    def shutdown
      @shutdown = true
      report(detailed: true)
      Thread.kill(@heartbeat)
      Thread.kill(@offset_manager)
      @workers.each_value(&:shutdown)
    end

    private

    def offset_ack(partition)
      @semaphore.synchronize do
        offset_ack_unsafe(partition)
      end
    end

    def offset_ack_unsafe(partition)
      retry_count = 0
      max_retries = ENV.fetch('WORKSHAPER_MAX_RETRIES', 10).to_i

      begin
        @total_acked ||= 0

        completed = @completed_offsets[partition]
        received = @received_offsets[partition]

        offset = completed.first
        while received.any? && received.first == offset
          result = @ack.call(partition, offset)

          if result.is_a?(Exception)
            if retry_count < max_retries
              retry_count += 1
              sleep(0.1 * retry_count) # Backoff exponencial
              next
            end

            WorkShaper.logger.error({
              event: 'partition_ack_failure',
              partition: partition,
              offset: offset,
              error: result.message,
              retry_count: retry_count
            })

            raise result
          end

          @total_acked += 1
          @last_ack[partition] = offset

          completed.delete(offset)
          received.delete(offset)
          offset = completed.first
        end
      rescue StandardError => e
        WorkShaper.logger.error({
          event: 'partition_processing_error',
          partition: partition,
          error: e.message,
          backtrace: e.backtrace.join("\n")
        })
        raise e
      end
    end

    def gather_partition_stats
      stats = {}
      @received_offsets.each do |partition, offsets|
        stats[partition] = {
          received_count: offsets.size,
          last_processed: @last_ack[partition],
          completed_count: (@completed_offsets[partition] || []).size,
          failure_count: @partition_failures[partition],
          blocked: partition_blocked?(partition)
        }
      end
      stats
    end

    def pause_on_overrun
      overrun = lambda do
        @total_enqueued.to_i - @total_acked.to_i > @max_in_queue
      end

      # We have to be careful here to avoid a deadlock. Another thread may be waiting
      # for the mutex to ack and remove offsets. If we wrap enqueue in a synchronize
      # block, that would lead to a deadlock. Here the sleep allows other threads
      # to wrap up.
      sleep 0.005 while @semaphore.synchronize { overrun.call }
    end

    def partition_blocked?(partition)
      @partition_blocks ||= {}
      return false unless @partition_blocks[partition]

      if Time.now - @partition_blocks[partition] > @partition_backoff_time
        @partition_blocks.delete(partition)
        @partition_failures[partition] = 0
        return false
      end
      true
    end

    def block_partition(partition)
      @partition_blocks ||= {}
      @partition_blocks[partition] ||= Time.now
    end
  end
end
