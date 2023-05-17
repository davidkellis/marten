require "ulid"

module Marten
  module DB
    module Field
      class ULID < Base
        getter default

        def initialize(
          @id : ::String,
          @primary_key = false,
          @default : ::ULID? = nil,
          @blank = false,
          @null = false,
          @unique = false,
          @index = false,
          @db_column = nil
        )
        end

        def from_db(value) : ::ULID?
          case value
          when Nil
            value.as?(Nil)
          when ::String
            ::ULID.new(value.as(::String))
          when ::UUID
            ::ULID.new(value.as(::UUID))
          else
            raise_unexpected_field_value(value)
          end
        end

        def from_db_result_set(result_set : ::DB::ResultSet) : ::ULID?
          from_db(result_set.read(Nil | ::String | ::UUID | ::UUID))
        end

        def to_column : Marten::DB::Management::Column::Base?
          Marten::DB::Management::Column::String.new(
            name: db_column!,
            max_size: 26,
            primary_key: primary_key?,
            null: null?,
            unique: unique?,
            index: index?,
            default: to_db(default)
          )
        end

        def to_db(value) : ::DB::Any
          case value
          when Nil
            nil
          when ::ULID
            value.to_s
          else
            value.to_s || raise_unexpected_field_value(value)
          end
        end

        def validate(record, value)
          return if value.nil?
          return if value.as?(::ULID)

          if (v = value.as?(::String))
            begin
              return if ::ULID.new(v)
            rescue ArgumentError
            end
          end

          record.errors.add(id, I18n.t("marten.db.field.ulid.errors.invalid"))
        end
      end
    end
  end
end
