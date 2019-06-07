module PGPQ

  class JobProcessor

    attr_reader :client, :queue_name, :count

    def initialize(client, queue_name, opts={})
      @client = client
      @queue_name = queue_name
      @count = opts[:count] || 5
      @output_queue_name = opts[:output_queue_name]

      @min_priority = nil
      @output_queue = nil
      @last_report_at = Time.now - 60
      @last_min_pri_update_at = Time.now - 60
    end

    def logger
      @client.logger
    end

    def report_queue
      queue = @client.get_queue(name: @queue_name)
      logger.info "QUEUE UPDATE (#{queue.name}): #{queue.jobs_count} jobs waiting"
      @last_report_at = Time.now
    end

    def update_min_priority
      begin
        q = @client.get_queue(name: @output_queue_name)
        raise "Output queue #{@output_queue_name} not found" if q.nil?
        @output_queue = q
        # check if close to full
        if q.jobs_count.to_f / q.capacity.to_f > 0.95
          @min_priority = q.min_priority + 1
          logger.info "QUEUE UPDATE: Min priority set to #{@min_priority}"
        elsif @min_priority != nil
          @min_priority = nil
          logger.info "QUEUE UPDATE: Min priority cleared"
        end
      rescue => ex
        logger.error ex.message
      end
      @last_min_pri_update_at = Time.now
    end

    def process_queue(opts={})
      loop do
        # log from queue
        if logger && secs_since(@last_report_at) > 30
          report_queue
        end

        # update min priority
        if @output_queue_name.present? && secs_since(@last_min_pri_update_at) > 30
          update_min_priority
        end

        # fetch jobs
        jobs = fetch_jobs
        if jobs.length == 0
          if opts[:continuous] == true
            sleep(opts[:sleep] || 5)
            next
          else
            break
          end
        end
        jobs.each do |job|
          process_job(job)
          release_job(job)
        end
      end
    end

    def fetch_jobs
      fopts = {queue_name: @queue_name, count: @count}
      fopts[:min_priority] = @min_priority if !@min_priority.nil?
      jobs = @client.dequeue_jobs(fopts)
      #puts jobs
      return jobs
    end

    def process_job(job)
      raise "process_job not implemented"
    end

    def release_job(job)
      @client.release_job(job.id)
    end

    def secs_since(t)
      return Time.now.to_i - t.to_i
    end

  end

end
