module StarEtl
  class Fact < Base
    
    attr_accessor :source, :destination, :time_dimension, :time_window, :column_map, :conditions, :batch_size, :batch_key
    
    def initialize
      @batch_size    = StarEtl.options[:batch_size]
      @batch_key     = "pk_id"
      @conditions    = []
      @column_map    = {}
      @ready_to_stop = false
    end
    
    def time_window=(seconds)
      raise "You have not set the time dimension column yet!" unless @time_dimension
      @column_map.merge! :fk_time_dimension => "(#{@time_dimension} / #{seconds}) * #{seconds}"
    end
    
    def run!
      print_summary
      
      @cols, @vals = *@column_map.stringify_keys.to_a.transpose
      
      
      until @ready_to_stop
        
        @conditions << get_batch_condition
        
        insert_sql = %Q{
          INSERT INTO #{@destination} #{@cols.join(',')}
          SELECT #{@vals.join(',')}
          FROM #{@destination}
          WHERE (#{@conditions.join(") AND (")})
        }
        
        debug insert_sql

      end
      
    end
    
    private
    
    def print_summary
      get_id_range
      puts %Q{SELECT count(*) from #{source} WHERE #{@id_range.call}}
      total = sql(%Q{SELECT count(*) from #{source} WHERE #{@id_range.call}}).first["count"]
      @total_chunks = (total.to_i / @batch_size) + 1
      puts "Extracting from #{total} total records from #{source} in #{@total_chunks} chunks"
      @completed = 0
    end
    
    def get_batch_condition
      ss             = %Q{SELECT #{@batch_key} from #{source} WHERE #{@id_range.call} order by #{@batch_key} ASC limit #{@batch_size}}
      debug ss
      records        = sql(ss)
      @ready_to_stop = records.size < @batch_size
      @last_id       = records.last[@batch_key].to_i unless records.empty?
      
      debug "last id is now #{@last_id}"
      
      "#{@batch_key} >= #{records.first[@batch_key]} AND #{@batch_key} =< #{@last_id}"
    end
    
    def get_id_range
      get_last_id
      @id_range = lambda {"(#{@batch_key} > #{@last_id} AND #{@batch_key} < #{@_to_id_})"}
    end
    
    def get_last_id
      info = sql(%Q{SELECT * from etl_info WHERE table_name = '#{source}' })
      @last_id = if info.empty?
        sql(%Q{INSERT INTO etl_info (last_id, table_name) VALUES (0, '#{source}') })
        0
      else
        info.first["last_id"]
      end
      @_to_id_ = sql(%Q{SELECT #{@batch_key} FROM #{source} ORDER BY #{@batch_key} desc LIMIT 1}).first[@batch_key]
      
      if @last_id && @_to_id_
        sql(%Q{UPDATE etl_info SET last_id = #{@_to_id_} WHERE table_name = '#{source}'})
      end
    end
    
    
  end
end