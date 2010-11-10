module StarEtl
  module Helper
    
    LOG = Logger.new("/Users/sntjon/Desktop/sql.log")
    MUTEX = Mutex.new
    
    def sql(q)
      MUTEX.synchronize {
        LOG.debug q.inspect
        r = Extractor.connection.execute(q)
        LOG.debug r.inspect
        r
      }
    end
    
    def insert_record(table, record)
      sql(%Q{INSERT INTO #{table} (#{record.keys.join(", ")}) VALUES (#{prepare_values(record.values)});})
    end
    
    def prepare_values(values)
      values.map do |v|         
        if v == true || v == false
          v
        elsif v.to_s.to_i == v
          v
        else
          "'#{v}'"
        end
      end.join(", ")
    end
    
  end
end