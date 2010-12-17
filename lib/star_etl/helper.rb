## 
# Author:: Jon Druse (mailto:jdruse@centraldesktop.com)
# 
# = Description
# 
# The helper module.  Included in (almost) every class.  Create globally useful stuff here.
#
# == Change History
# 11/12/10:: added documentation Jon Druse (mailto:jdruse@centraldesktop.com)
### 
module StarEtl
  module Helper
    ## 
    # Author:: Jon Druse (mailto:jdruse@centraldesktop.com)
    #
    # Runs +q+ on the connected database
    #
    # == Parameters
    #
    # * +q+ - String - the actual sql query to run
    #
    # == Returns
    #
    # The array of records returned
    #
    # == Change History
    # 11/12/10:: added documentation Jon Druse (mailto:jdruse@centraldesktop.com)
    ### 
    def sql(q)
      StarEtl.connection.execute(q)
    end
    
    ## 
    # Author:: Jon Druse (mailto:jdruse@centraldesktop.com)
    #
    # If the debug option is set to true (it's false by default) this just puts the message to STDOUT
    #
    # == Parameters
    #
    # * +msg+ - String - the message to print
    #
    # == Change History
    # 11/12/10:: added documentation Jon Druse (mailto:jdruse@centraldesktop.com)
    ### 
    def debug(msg)
      puts msg if StarEtl.options[:debug]
    end

    def get_id_range(sequence, source)
      get_last_id(sequence, source)
      @nothing_new = true if @last_id.to_i == @_to_id_.to_i
      @id_range = lambda {"source.#{@primary_key} BETWEEN #{@last_id} AND #{@_to_id_}"}
    end
    
    def get_last_id(sequence, source)
      info = sql(%Q{SELECT * from etl_info WHERE table_name = '#{source}' })
      @last_id = if info.empty?
        sql(%Q{INSERT INTO etl_info (last_id, table_name) VALUES (0, '#{source}') })
        0
      else
        info.first["last_id"]
      end
      ss = %Q{SELECT nextval('#{sequence}')}
      debug(ss)
      @_to_id_ = sql(ss).first["nextval"]
      debug("to_id = #{@_to_id_}")
      if @last_id && @_to_id_
        sql(%Q{UPDATE etl_info SET last_id = #{@_to_id_} WHERE table_name = '#{source}'})
      end
    end
    
  end
end