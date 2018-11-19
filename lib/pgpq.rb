require "pgpq/version"
require "pgpq/client"
require "pgpq/job_processor"

module PGPQ
  # Your code goes here...
  def self.time_priority
    Time.now.to_i * -1
  end
end
