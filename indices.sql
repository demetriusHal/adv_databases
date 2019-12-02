--- q1 q2---
create index time_index on log(time)
--- possibly unecessary ---
create index type_index on log(type)

--- q3 ---
create index source_ip_index on log(source_ip)

--- q4,q5,q6 VERY OPTIONAL ---
create index block_index on blocks(block_requested)
create index referer_index on access(referer)
create index resourcec_index on access(resource)

--- q7 ---
create index size_index on access(response_size)

--- q10 ---
create index free_text_index using gist on access(user_string)

--- q11-13 ---
create index method_index on access(http_method)
