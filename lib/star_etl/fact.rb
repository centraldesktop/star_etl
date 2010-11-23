module StarEtl
  class Fact < Base
    
    attr_accessor :source, :destination, :time_dimension, :time_window, :column_map, :conditions, :group_by, :aggregate
    
    def initialize
      @primary_key   = StarEtl.options[:primary_key]
      @conditions    = []
      @column_map    = {}
      @ready_to_stop = false
      @aggregate     = false
    end
    
    def time_window=(seconds)
      raise "You have not set the time dimension column yet!" unless @time_dimension
      @column_map.merge! :fk_time_dimension => "(#{@time_dimension} / #{seconds}) * #{seconds}"
    end
    
    def column_map=(hash)
      @column_map.merge! hash
    end
    
    def run!
      print_summary
      @cols, @vals = *@column_map.stringify_keys.to_a.transpose
      
      insert_sql = %Q{
        INSERT INTO #{@destination} (#{@cols.join(',')})
        SELECT #{@vals.join(',')}
        FROM #{@source}
        WHERE (#{@conditions.join(") AND (")})
      }
      
      debug insert_sql
      sql(insert_sql)
    end
    
    private
    
    def print_summary
      get_id_range
      if @nothing_new
        puts "No new records in #{source}"
      else
        total = sql(%Q{SELECT count(*) as "total" from #{source} WHERE #{@id_range.call}}).first["total"]
        puts "Extracting from #{total} total records from #{source}"
      end
    end
    
    def get_id_range
      get_last_id
      @nothing_new = true if @last_id.to_i == @_to_id_.to_i
      @id_range = lambda {"#{@primary_key} BETWEEN #{@last_id} AND #{@_to_id_}"}
    end
    
    def get_last_id
      info = sql(%Q{SELECT * from etl_info WHERE table_name = '#{source}' })
      @last_id = if info.empty?
        sql(%Q{INSERT INTO etl_info (last_id, table_name) VALUES (0, '#{source}') })
        0
      else
        info.first["last_id"]
      end
      @_to_id_ = sql(%Q{SELECT max(#{@primary_key}) as "max" FROM #{source}}).first["max"]
      
      if @last_id && @_to_id_
        sql(%Q{UPDATE etl_info SET last_id = #{@_to_id_} WHERE table_name = '#{source}'})
      end
    end
    
  end
end