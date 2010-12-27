module StarEtl
  class Fact < Base
    
    attr_accessor :source, :destination, :time_dimension, :time_window, :column_map
    attr_accessor :conditions, :group_by, :aggregate, :primary_key
    
    def initialize(agg=false)
      @primary_key   = StarEtl.options[:primary_key]
      @conditions    = []
      @column_map    = {}
      @aggregate     = agg
      @group_by      = []
    end
    
    def time_window=(seconds)
      raise "You have not set the time dimension column yet!" unless @time_dimension
      
      s = "(#{@time_dimension} / #{seconds}) * #{seconds}"
      @column_map.merge! :fk_time_dimension => s
    end
    
    def column_map=(hash)
      @column_map.merge! hash
    end
    
    def run!
      print_summary
      return if @nothing_new
      
      @cols, @vals = *@column_map.stringify_keys.to_a.transpose
      group = @group_by.clone
      group.unshift(@column_map[:fk_time_dimension]) if @aggregate
      @conditions.unshift(@id_range.call)
      
      insert_sql = %Q{
        INSERT INTO #{@destination} (#{@cols.join(',')})
        SELECT #{@vals.join(',')}
        FROM #{@source} source
        WHERE (#{@conditions.join(") AND (")})
        #{"GROUP BY #{group.join(',')}" unless group.empty?}
      }
      
      debug insert_sql
      sql(insert_sql)
    end
    
    def sequence
      @sequence || %Q{#{self.source}_#{self.primary_key}_seq}
    end
    
    private
    
    def print_summary
      
      get_id_range(source)
      
      if @nothing_new
        puts "No new records in #{source}"
      # else
      #   total = sql(%Q{SELECT count(*) as "total" from #{source} WHERE #{@id_range.call}}).first["total"]
      #   puts "Extracting from #{total} total records from #{source}"
      end
    end
    
  end
end