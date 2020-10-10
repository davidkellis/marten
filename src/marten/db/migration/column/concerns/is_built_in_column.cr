module Marten
  module DB
    abstract class Migration
      module Column
        # :nodoc:
        module IsBuiltInColumn
          macro included
            def sql_type(connection : Connection::Base) : ::String
              connection.column_type_for_built_in_column({{ @type.name.stringify }}) % db_type_parameters
            end

            def sql_type_suffix(connection : Connection::Base) : ::String?
              connection.column_type_suffix_for_built_in_column({{ @type.name.stringify }})
            end

            private def db_type_parameters
              nil
            end
          end
        end
      end
    end
  end
end
