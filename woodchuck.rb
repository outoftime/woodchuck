require 'rubygems'
require 'redis'
require 'system_timer'
require 'uuidtools'
require 'json'

module Woodchuck
  class Database
    attr_reader :redis

    def initialize(redis = Redis.new)
      @redis = redis
      @maps = {}
    end

    def add(doc)
      id = @redis.incr("wchk:nextid")
      @redis.set("wchk:doc:#{id}", JSON.dump(doc.merge('_id' => id)))
      @maps.keys.each do |map_name|
        @redis.sadd("wchk:pend:#{map_name}", id)
      end
      id
    end

    def get(id)
      JSON.parse(@redis.get("wchk:doc:#{id}"))
    end

    def map(map_name, map_function)
      @redis.set("wchk:mapfunc:#{map_name}", map_function)
      load_map(map_name)
      each_doc_id do |id|
        @redis.sadd("wchk:pend:#{map_name}", id)
      end
    end

    def lookup(map_name, key, limit = -1)
      repair(map_name)
      start_rank, end_rank = nil, nil
      case key
      when Range
        start_rank, end_rank =
          Woodchuck.key_to_rank(key.first), Woodchuck.key_to_rank(key.last)
      else
        start_rank = end_rank = Woodchuck.key_to_rank(key)
      end
      @redis.zrangebyscore("wchk:map:#{map_name}", start_rank, end_rank).map do |doc|
        JSON.parse(doc)
      end
    end

    def all(map_name, options = {})
      offset = options[:offset] || 0
      limit = options[:limit]
      first = offset
      last = limit ? offset+limit-1 : -1
      repair(map_name)
      @redis.zrange("wchk:map:#{map_name}", first, last).map do |doc|
        JSON.parse(doc)
      end
    end

    private

    def repair(map_name)
      map = load_map(map_name)
      parallelize(4) do
        while id = @redis.spop("wchk:pend:#{map_name}")
          doc = get(id)
          map.consume(doc) do |rank, value|
            @redis.zadd("wchk:map:#{map_name}", rank, JSON.dump(value))
          end
        end
      end
    end

    def parallelize(count)
      pids = []
      while pids.length < count
        if pid = fork
          pids << pid
        else
          @redis = Redis.new
          yield
          exit
        end
      end
      pids.each { |pid| Process.waitpid(pid) }
    end

    def load_map(map_name)
      @maps[map_name.to_sym] ||=
        Map.new(map_name, @redis.get("wchk:mapfunc:#{map_name}"))
    end

    def each_doc_id
      @redis.keys("wchk:doc:*").each do |key|
        yield key[/[^:]+$/]
      end
    end
  end

  class Map
    def initialize(name, map_function_source)
      @name = name
      map_function_proc = eval(map_function_source) #FIXME danger!
      (class <<self ; self ; end).module_eval do
        define_method(:perform, &map_function_proc)
        private :perform
      end
    end

    def consume(document)
      @pairs = []
      perform(document)
      @pairs.each do |pair|
        yield Woodchuck.key_to_rank(pair.first), pair.last
      end
    end

    private

    def emit(key, value)
      @pairs << [key, value]
    end
  end

  def self.key_to_rank(key)
    rank =
      case key
      when Range
        raise
      when Float
        key
      when Numeric
        key.to_f
      when String
        num = 0
        8.times do |i|
          num = (num << 8) | (key[i] || 0)
        end
        num.to_f
      else
        key_to_rank(key.to_s)
      end
    rank
  end
end
