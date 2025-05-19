module WorkShaper
  # The worker that runs the stuff
  class Worker
    include ::LoggerFactory
    # rubocop:disable Metrics/ParameterLists
    # rubocop:disable Layout/LineLength
    # @param work [Lambda] Lambda that we will #call(message) to execute work.
    # @param on_done [Lambda] Lambda that we #call(partition, offset) when work is done.
    # @param on_error [Lambda] Lambda that we #call(exception) if an error is encountered.
    def initialize(work, on_done, ack_handler, on_error, last_ack, offset_stack, semaphore, max_in_queue)
      @jobs = []
      @work = work
      @on_done = on_done
      @ack_handler = ack_handler
      @on_error = on_error
      @last_ack = last_ack
      @completed_offsets = offset_stack
      @semaphore = semaphore
      @max_in_queue = max_in_queue
      @thread_pool = Concurrent::FixedThreadPool.new(
        ENV.fetch('WORKSHAPER_WORKER_THREADS_POOL_SIZE', 10).to_i,
        auto_terminate: false,
        max_queue: ENV.fetch('WORKSHAPER_WORKER_QUEUE_SIZE', 1000).to_i,
        fallback_policy: :caller_runs
      )
    end

    # rubocop:enable Metrics/ParameterLists
    # rubocop:enable Layout/LineLength

    def enqueue(message, partition, offset)
      @thread_pool.post do
        begin
          ActiveRecord::Base.connection_pool.with_connection do
            start_time = Time.now

            @work.call(message, partition, offset)
            @on_done.call(message, partition, offset)

            @semaphore.synchronize do
              (@completed_offsets[partition] ||= SortedSet.new) << offset
            end

            WorkShaper.logger.info({
              event: 'message_processed',
              partition: partition,
              offset: offset,
              processing_time: Time.now - start_time
            })
          end
        rescue StandardError => e
          WorkShaper.logger.error({
            event: 'message_processing_error',
            partition: partition,
            offset: offset,
            error: e.message,
            backtrace: e.backtrace[0..5]
          })

          @on_error.call(e, message, partition, offset)

          retry_count = 0
          max_retries = ENV.fetch('WORKSHAPER_MESSAGE_MAX_RETRIES', 3).to_i

          if retry_count < max_retries
            retry_count += 1
            sleep(0.1 * retry_count) # Backoff exponencial
            retry
          end
        ensure
          ActiveRecord::Base.connection_pool.release_connection if ActiveRecord::Base.connection_pool
        end
      end
    rescue Concurrent::RejectedExecutionError => e
      WorkShaper.logger.error({
        event: 'thread_pool_rejected',
        partition: partition,
        offset: offset,
        error: e.message
      })
      raise e
    end

    def shutdown
      WorkShaper.logger.info({ event: 'worker_shutdown_started' })

      @thread_pool.shutdown

      timeout = ENV.fetch('WORKSHAPER_SHUTDOWN_TIMEOUT', 30).to_i
      if @thread_pool.wait_for_termination(timeout)
        WorkShaper.logger.info({ event: 'worker_shutdown_completed' })
      else
        WorkShaper.logger.warn({
          event: 'worker_shutdown_timeout',
          remaining_tasks: @thread_pool.queue_length
        })
        @thread_pool.kill
      end
    end

    private
  end
end
