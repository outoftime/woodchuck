begin
  require 'redis'
  require 'system_timer'
  require 'v8'
rescue LoadError => e
  retry if require 'rubygems'
  raise e
end

module Woodchuck
  class Database
    attr_reader :redis

    def initialize(redis = Redis.new, log = Logger.new($stdout, Logger::DEBUG))
      @redis, @log = redis, log
      @maps = {}
    end

    def add(doc)
      @log.info("ADD")
      id = store(doc)
      @maps.keys.each { |map_name| @redis.sadd("wchk:pend:#{map_name}", id) }
      id
    end

    def update(id, doc)
      @log.info("UPDATE #{id}")
      store(id, doc)
      @maps.keys.each do |map_name|
        @redis.sadd("wchk:pend:#{map_name}", id)
        @redis.sadd("wchk:penddel:#{map_name}", id) #XXX can we use SMOVE?
      end
    end

    def delete(id)
      @log.info("DELETE #{id}")
      @redis.del("wchk:doc:#{id}")
      @maps.keys.each do |map_name|
        @redis.srem("wchk:pend:#{map_name}", id)
        @redis.sadd("wchk:penddel:#{map_name}", id) #XXX can we use SMOVE?
      end
    end

    def get(id)
      @log.info("GET #{id}")
      @redis.get("wchk:doc:#{id}")
    end

    def map(map_name, map_function)
      @log.info("MAP #{map_name}")
      @redis.set("wchk:mapfunc:#{map_name}", map_function)
      load_map(map_name)
      each_doc_id do |id|
        @redis.sadd("wchk:pend:#{map_name}", id)
      end
    end

    def lookup(map_name, key, limit = -1)
      @log.info("LOOKUP #{map_name}")
      repair(map_name)
      start_rank, end_rank = nil, nil
      case key
      when Range
        start_rank, end_rank =
          Woodchuck.key_to_rank(key.first), Woodchuck.key_to_rank(key.last)
      else
        start_rank = end_rank = Woodchuck.key_to_rank(key)
      end
      @redis.zrangebyscore("wchk:map:#{map_name}", start_rank, end_rank).map do |id|
        get(id)
      end
    end

    def all(map_name, options = {})
      @log.info("ALL #{map_name}")
      offset = options[:offset] || 0
      limit = options[:limit]
      first = offset
      last = limit ? offset+limit-1 : -1
      repair(map_name)
      @redis.zrange("wchk:map:#{map_name}", first, last).map do |id|
        get(id)
      end
    end

    def truncate
      @log.info("TRUNCATE")
      @redis.keys("wchk:*").each do |key|
        @redis.del(key)
      end
    end

    private

    def store(doc, id = nil)
      id ||= @redis.incr("wchk:nextid")
      @redis.set("wchk:doc:#{id}", doc)
      id
    end

    def repair(map_name)
      while id = @redis.spop("wchk:penddel:#{map_name}")
        while value_id = @redis.spop("wchk:metamap:#{map_name}:#{id}")
          @redis.zrem("wchk:map:#{map_name}", value_id)
        end
      end

      map = load_map(map_name)
      while id = @redis.spop("wchk:pend:#{map_name}")
        doc = get(id)
        map.consume(doc) do |rank, value|
          value_id = store(value)
          @redis.zadd("wchk:map:#{map_name}", rank, value_id)
          @redis.sadd("wchk:metamap:#{map_name}:#{id}", value_id)
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
      @context = V8::Context.new
      @context['emit'] = lambda do |key, value|
        hash = {}
        value.each do |name, content|
          hash[name] = content
        end
        emit(key, hash)
      end
      @map_function = @context.eval("f = #{map_function_source}")
    end

    def perform(document)
      @map_function.call(document)
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
