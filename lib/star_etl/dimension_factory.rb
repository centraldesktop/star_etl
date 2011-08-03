module StarEtl
  class DimensionFactory < Base
    
    def self.id_ranges
      @id_ranges ||= {}
    end
    
    attr_accessor :sources, :dimensions
    
    def initialize
      @primary_key = StarEtl.options[:primary_key]
      @dimensions  = {}
      @sources  = []
    end
    
    def []=(k,v)
      @dimensions[k] = v
    end
    
    def run!
      
      @sources.each do |source|
        
        source_info_key = "#{"#{source.gsub("\"",'')}_dimension"}"
        get_id_range(source, source_info_key)
        
        
        @dimensions.each_pair do |name, config|
          puts "Synchronizing #{name} from #{source}"
          
          opts = config.clone
          
          conditions = []
          conditions << @id_range.call
          conditions << opts.delete(:conditions)
          join = opts.delete(:join)
          group = opts.delete(:group)
          
          cols, vals = *opts.stringify_keys.to_a.transpose

          # INSERT INTO #{name} (#{cols.join(',')})
          # SELECT #{vals.join(',')}
          # FROM #{source} source
          # LEFT OUTER JOIN #{name} dimension ON (#{join})
          # WHERE (#{conditions.compact.join(") AND (")})
          # #{"GROUP BY #{group}" if group}
          
          proc_name = "sync_#{name.split(".").last}"
        
          create_stored_proc = %Q{            
            CREATE OR REPLACE FUNCTION "#{proc_name}"() RETURNS VOID AS
              
              $BODY$
                DECLARE 
                  insert_cursor NO SCROLL CURSOR FOR SELECT #{opts.collect {|k, v| "#{v} as #{k}" }.join(',')} FROM #{source} source WHERE (#{conditions.compact.join(") AND (")}) #{"GROUP BY #{group}" if group};
                  
                BEGIN
                  FOR record IN insert_cursor LOOP
                    BEGIN
                      INSERT INTO #{name} (#{cols.join(',')}) VALUES (#{cols.collect {|c| "record.#{c}" }.join(',')});
                    EXCEPTION WHEN unique_violation THEN
                      
                    END;

                END LOOP;
                RETURN;
            END;
            $BODY$
                LANGUAGE 'plpgsql' VOLATILE
                COST 1;            
          }
          
          debug create_stored_proc
          sql(create_stored_proc)
          
          sql("SELECT #{proc_name}();")
          sql("DROP FUNCTION #{proc_name}();")
        end
      end

    end

    def get_id_range(source, source_key)
      if self.class.id_ranges.has_key?(source)
        @last_id, @_to_id_ = *self.class.id_ranges[source]
        @nothing_new = true if @last_id.to_i == @_to_id_.to_i
        @id_range = lambda {"source.#{@primary_key} BETWEEN #{@last_id} AND #{@_to_id_}"}
      else
        super
        self.class.id_ranges[source] = [@last_id, @_to_id_]
      end
    end
        
  end
end
