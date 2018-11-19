require 'hashie'
require 'faraday'

module PGPQ

  class Client

    def initialize(opts={})
      @url = opts[:url]
      @conn = Faraday.new(url: @url) do |f|
        f.request :url_encoded
        f.response :logger
        f.adapter Faraday.default_adapter
      end
    end

    def enqueue_job(opts={})
      rdata = {}
      rdata[:queue_name] = opts[:queue_name]
      rdata[:quid] = opts[:quid] if opts.key?(:quid)
      rdata[:priority] = opts[:priority] if opts.key?(:priority)
      if opts[:data]
        rdata[:data] = opts[:data].to_json
      end
      resp = @conn.post "/enqueue", rdata
      res = parse_response(resp)
      return res.data
    end

    def dequeue_jobs(opts={})
      resp = @conn.post "/dequeue", opts
      res = parse_response(resp)
      return res.data
    end

    def release_job(id)
      resp = @conn.post "/release", {job_id: id}
      res = parse_response(resp)
      return res.data
    end

    def peek_jobs(opts={})
      resp = @conn.get "/peek", opts
      res = parse_response(resp)
      return res.data
    end

    def parse_response(resp)
      begin
        jsp = JSON.parse(resp.body)
        res =  Hashie::Mash.new(jsp)
      rescue => ex
        Rails.logger.info ex.message
        Rails.logger.info ex.backtrace.join("\n")
        res = Hashie::Mash.new(success: false, error: "Response could not be parsed. (#{ex.message})")
      end
      raise res.error if !res.success
      return res
    end

  end

end
