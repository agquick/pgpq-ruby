module PGPQ

  class JobProcessor

    def initialize(client, queue_name, opts={})
      @client = client
      @queue_name = queue_name
      @count = opts[:count] || 5
    end

    def logger
      @client.logger
    end

    def process_queue(opts={})
      qrt = Time.now - 60
      loop do
        # log from queue
        if @logger && (Time.now.to_i - qrt.to_i) > 30
          queue = @client.get_queue(name: @queue_name)
          @logger.info "QUEUE UPDATE (#{queue.name}): #{queue.jobs_count} jobs waiting"
          qrt = Time.now
        end
        jobs = get_jobs
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

    def get_jobs
      jobs = @client.dequeue_jobs(queue_name: @queue_name, count: @count)
      #puts jobs
      return jobs
    end

    def process_job(job)
      raise "process_job not implemented"
    end

    def release_job(job)
      @client.release_job(job.id)
    end

  end

end
