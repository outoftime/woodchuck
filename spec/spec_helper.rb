begin
  require 'spec'
rescue LoadError => e
  retry if require('rubygems')
  raise e
end

$: << File.expand_path('../../lib', __FILE__)
require 'woodchuck/client'

Spec::Runner.configure do |config|
  config.before(:each) do
    @db = Woodchuck::Client.new
    @db.truncate
  end
end
