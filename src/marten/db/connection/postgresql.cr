module Marten
  module DB
    module Connection
      class PostgreSQL < Base
        def column_type_for_built_in_column(id)
          BUILT_IN_COLUMN_TO_DB_TYPE_MAPPING[id]
        end

        def column_type_suffix_for_built_in_column(id)
          nil
        end

        def insert(table_name : String, values : Hash(String, ::DB::Any), pk_field_to_fetch : String? = nil) : Int64?
          column_names = values.keys.map { |column_name| "#{quote(column_name)}" }.join(", ")
          numbered_values = values.keys.map_with_index { |_c, i| parameter_id_for_ordered_argument(i + 1) }.join(", ")
          statement = "INSERT INTO #{quote(table_name)} (#{column_names}) VALUES (#{numbered_values})"
          statement += " RETURNING #{quote(pk_field_to_fetch)}" unless pk_field_to_fetch.nil?

          new_record_id = nil

          open do |db|
            if pk_field_to_fetch
              new_record_id = db.scalar(statement, args: values.values).as(Int32 | Int64).to_i64
            else
              db.exec(statement, args: values.values)
            end
          end

          new_record_id
        end

        def introspector : Management::Introspector::Base
          Management::Introspector::PostgreSQL.new(self)
        end

        def left_operand_for(id : String, predicate) : String
          transformation = PREDICATE_TO_LEFT_OPERAND_TRANSFORMATION_MAPPING.fetch(predicate, nil)
          transformation.nil? ? id : (transformation % id)
        end

        def operator_for(predicate) : String
          PREDICATE_TO_OPERATOR_MAPPING[predicate]
        end

        def parameter_id_for_ordered_argument(number : Int) : String
          "$#{number}"
        end

        def quote_char : Char
          '"'
        end

        def schema_editor : Management::SchemaEditor::Base
          Management::SchemaEditor::PostgreSQL.new(self)
        end

        def scheme : String
          "postgres"
        end

        def update(
          table_name : String,
          values : Hash(String, ::DB::Any),
          pk_column_name : String,
          pk_value : ::DB::Any
        ) : Nil
          column_names = values.keys.map_with_index do |column_name, i|
            "#{quote(column_name)}=#{parameter_id_for_ordered_argument(i + 1)}"
          end.join(", ")

          statement = "UPDATE #{quote(table_name)} SET #{column_names} " \
                      "WHERE #{quote(pk_column_name)}=#{parameter_id_for_ordered_argument(values.size + 1)}"

          open do |db|
            db.exec(statement, args: values.values + [pk_value])
          end
        end

        private BUILT_IN_COLUMN_TO_DB_TYPE_MAPPING = {
          "Marten::DB::Migration::Column::Auto"       => "serial",
          "Marten::DB::Migration::Column::BigAuto"    => "bigserial",
          "Marten::DB::Migration::Column::BigInt"     => "bigint",
          "Marten::DB::Migration::Column::Bool"       => "boolean",
          "Marten::DB::Migration::Column::DateTime"   => "timestamp with time zone",
          "Marten::DB::Migration::Column::ForeignKey" => "bigint",
          "Marten::DB::Migration::Column::Int"        => "integer",
          "Marten::DB::Migration::Column::String"     => "varchar(%{max_size})",
          "Marten::DB::Migration::Column::Text"       => "text",
          "Marten::DB::Migration::Column::UUID"       => "uuid",
        }

        private PREDICATE_TO_LEFT_OPERAND_TRANSFORMATION_MAPPING = {
          "icontains"   => "UPPER(%s)",
          "iendswith"   => "UPPER(%s)",
          "iexact"      => "UPPER(%s)",
          "istartswith" => "UPPER(%s)",
        }

        private PREDICATE_TO_OPERATOR_MAPPING = {
          "contains"    => "LIKE %s",
          "endswith"    => "LIKE %s",
          "exact"       => "= %s",
          "icontains"   => "LIKE UPPER(%s)",
          "iendswith"   => "LIKE UPPER(%s)",
          "iexact"      => "LIKE UPPER(%s)",
          "istartswith" => "LIKE UPPER(%s)",
          "startswith"  => "LIKE %s",
        }
      end
    end
  end
end
