module StarEtl
  module Helper
    
    MUTEX = Mutex.new
    ERR_LOG = Logger.new("/Users/sntjon/Desktop/logs/error.log")
    
    def sql(q)
      Extractor.connection.execute(q)
    end
    
    def insert_record(table, record)
      record.delete_if { |key, val| val.nil? || val == "" }
      # this guarantees that the cols and values are in the same order
      a          = record.to_a
      cols, vals = a.map(&:shift), a.map(&:shift)      
      sql(%Q{INSERT INTO #{table} (#{cols.join(", ")}) VALUES (#{prepare_values(vals)});})
    end
    
    def debug(msg)
      puts msg if StarEtl::Extractor.options[:debug]
    end
    
    def prepare_values(values)
      values.map do |v| 
        case v
        when String
          "'#{v}'"
        else
          v
        end
      end.join(", ")
    end
    
    def round_down_to_minute(stamp)
      (stamp.to_f / 60).floor * 60
    end
    
  end
end