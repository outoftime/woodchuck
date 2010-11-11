require './fdb'


def time(desc)
  time = Time.now.to_f
  yield
  puts "#{desc} in #{(Time.now.to_f - time) * 1000}ms"
end

DB = Woodchuck::Database.new
redis = DB.redis
redis.keys("wchk:*").each { |key| redis.del(key) }

3.times do
  DB.add('name' => 'hey')
end

DB.map(:by_name, <<-RUBY)
  lambda { |doc| emit doc['name'], doc }
RUBY

puts DB.all(:by_name, :offset => 1).inspect
