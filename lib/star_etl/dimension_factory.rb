module StarEtl
  class DimensionFactory < Base
    
    attr_accessor :sources, :dimensions
    
    def initialize
      @dimensions = {}
    end
    
    def []=(k,v)
      @dimensions[k] = v
    end
    
    def run!
      @sources.each do |source|
        @dimensions.each_pair do |name, config|
          opts = config.clone
          
          conditions = ["dimension IS NULL"]
          conditions << opts.delete(:conditions)
          join = opts.delete(:join)
        
          cols, vals = *opts.stringify_keys.to_a.transpose
        
          insert_sql = %Q{
            INSERT INTO #{name} (#{cols.join(',')})
            SELECT #{vals.join(',')}
            FROM #{source} source
            LEFT OUTER JOIN #{name} dimension ON (#{join})
            WHERE (#{conditions.compact.join(") AND (")})
          }
          
          debug insert_sql
          
          sql(insert_sql)
        
        end
      end

    end
    
  end
end