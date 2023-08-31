{{
 config(materialized = 'streaming_table', file_format = 'delta',)
}}

select * from stream read_files(
    '{{ var("s3_path") }}/orders/'
  )