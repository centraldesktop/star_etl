module StarEtl
  class DimensionFactory < Base
    
    attr_accessor :source, :sources, :dimensions
    
    def initialize
      @primary_key = StarEtl.options[:primary_key]
      @dimensions  = {}
    end
    
    def []=(k,v)
      @dimensions[k] = v
    end
    
    def run!
      [@source,@sources].flatten.compact.each do |source|
        
        get_id_range(source)
        
        @dimensions.each_pair do |name, config|
          puts "Synchronizing #{name} from #{source}"
          
          opts = config.clone
          
          conditions = ["dimension IS NULL"]
          conditions << @id_range.call
          conditions << opts.delete(:conditions)
          join = opts.delete(:join)
          group = opts.delete(:group)
          
          cols, vals = *opts.stringify_keys.to_a.transpose
        
          insert_sql = %Q{
            INSERT INTO #{name} (#{cols.join(',')})
            SELECT #{vals.join(',')}
            FROM #{source} source
            LEFT OUTER JOIN #{name} dimension ON (#{join})
            WHERE (#{conditions.compact.join(") AND (")})
            #{"GROUP BY #{group}" if group}
          }
          
          debug insert_sql
          
          sql(insert_sql)
        
        end
      end

    end
    
    def get_id_range(source)
      get_last_id(source)
      @nothing_new = true if @last_id.to_i == @_to_id_.to_i
      @id_range = lambda {"source.#{@primary_key} BETWEEN #{@last_id} AND #{@_to_id_}"}
    end
    
    private
    
    def get_last_id(source)
      info = sql(%Q{SELECT * from etl_info WHERE table_name = '#{"#{source.gsub("\"",'')}_dimension"}' })
      @last_id = if info.empty?
        sql(%Q{INSERT INTO etl_info (last_id, table_name) VALUES (0, '#{"#{source.gsub("\"",'')}_dimension"}') })
        0
      else
        info.first["last_id"]
      end
      @_to_id_ = sql(%Q{SELECT max(#{@primary_key}) as "max" FROM #{source}}).first["max"]
      
      if @last_id && @_to_id_
        sql(%Q{UPDATE etl_info SET last_id = #{@_to_id_} WHERE table_name = '#{"#{source.gsub("\"",'')}_dimension"}'})
      end
    end
    
    
  end
end