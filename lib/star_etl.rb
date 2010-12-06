require 'star_etl/helper'
require 'star_etl/base'
require 'star_etl/fact'
require 'star_etl/dimension_factory'


module StarEtl
  
  class << self
    def connect!(db_config)
      @mutex = Mutex.new
      
      ActiveRecord::Base.establish_connection(db_config)
      @conn = ActiveRecord::Base.connection
    end
  
    def connection
      @mutex.synchronize { @conn }
    end
    
    def options!(hsh)
      defaults = {
        :primary_key => "id",
        :debug       => false
      }

      @options = defaults.merge(hsh)
    end
    
    def options
      @options
    end
    
    def setup(db_config, opts={})
      connect!(db_config)
      options!(opts)
      @facts  = []
    end
    
    def fact
      f = Fact.new
      yield f
      @facts << f
    end
    
    def aggregate
      f = Fact.new(true)
      yield f
      @facts << f
    end
    
    def dimension_factory
      d = DimensionFactory.new
      yield d
      
      d.run!
    end

    def start!
      started = Time.now
      @facts.each {|f| f.run! }
      puts "Finish in #{format_duration(Time.now - started)} "
    end

    private

    def format_duration(seconds)
      m, s = seconds.divmod(60)
      "#{m} minutes and #{'%.3f' % s} seconds" 
    end
    
  end
  
  
end


# ActiveRecord::Base.establish_connection(db_config)
# @conn = ActiveRecord::Base.connection
# @cols = %w(pk_id day_of_week day_of_month day_of_year hour minute month year quarter holiday weekend)
# 
# def sql(sql)
#   @conn.execute(sql)
# end
# 
# def dump!
#   sql(%Q{INSERT INTO time_dimension (#{@cols.join(',')}) VALUES #{@values.join(',')} })
#   STDOUT.print "."
#   STDOUT.flush
#   @values = []
# end
# 
# def format_duration(seconds)
#   m, s = seconds.divmod(60)
#   "#{m} minutes and #{'%.3f' % s} seconds" 
# end
# 
# 
# date = DateTime.new(2010, 01, 01, 00, 00, 00)
# end_date  = DateTime.new(2012, 01, 01, 00, 00, 00)
# 
# @values = []
# 
# started = Time.now
# 
# until date == end_date
#   val = []
#   
#   val << date.to_i
#   val += date.strftime("%w,%d,%j,%H,%M,%m,%Y").split(",")
#   val << [1,4,7,10].index([10, 7, 4, 1].detect { |m| m <= date.month }) + 1
#   val << false #!date.to_date.holiday?(:us)
#   val << [0,6].include?(date.wday)
#   
#   @values << "(#{val.join(",")})"
#   
#   dump! if @values.size == 1000
#   date += 1.minute
# end
# dump!
# 
# puts "Finished in #{format_duration(Time.now - started)} "
#