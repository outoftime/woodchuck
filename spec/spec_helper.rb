begin
  require 'spec'
rescue LoadError => e
  retry if require('rubygems')
  raise e
end

require File.expand_path('../../lib/woodchuck', __FILE__)

Spec::Runner.configure do |config|
  config.before(:each) do
    @db = Woodchuck::Database.new
    @db.truncate
  end
end
