module Marten
  module DB
    module Management
      module Column
        # Abstract base migration column implementation.
        abstract class Base
          getter name

          setter name
          setter primary_key

          def initialize(
            @name : ::String,
            @primary_key = false,
            @null = false,
            @unique = false,
            @index = false
          )
          end

          # Returns a copy of the column.
          abstract def clone

          # Returns the raw type of the column to use for the column at hand and a specific database connection.
          abstract def sql_type(connection : Connection::Base) : ::String

          # Returns true if an index should be created at the database level for the column.
          def index?
            @index
          end

          # Returns a boolean indicating whether the column can be null or not.
          def null?
            @null
          end

          # Returns a boolean indicating whether the column is a primary key.
          def primary_key?
            @primary_key
          end

          # Returns the raw type suffix of the column to use for the column at hand and a specific database connection.
          def sql_type_suffix(connection : Connection::Base) : ::String?
            nil
          end

          # Returns the column type identifier.
          def type : ::String
            Column.registry.key_for(self.class)
          end

          # Returns a boolean indicating whether the column value should be unique throughout the associated table.
          def unique?
            @unique
          end
        end
      end
    end
  end
end
