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
    
  end
end