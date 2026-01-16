{
all_cols = { {{all_cols}} }
int_cols = { {{int_cols}} }
bool_cols = { {{bool_cols}} }
str_cols = { {{str_cols}} }

math.randomseed({{seed}})

int_ops = { "=", "!=", "<", "<=", ">", ">=" }
eq_ops = { "=", "!=" }
logic_ops = { "AND", "OR" }

function pick(list)
  return list[math.random(#list)]
end

function pick_cols(list)
  local n = math.random(#list)
  local used = {}
  local out = {}
  while #out < n do
    local col = list[math.random(#list)]
    if used[col] == nil then
      used[col] = true
      table.insert(out, col)
    end
  end
  return table.concat(out, ", ")
end

function rand_int()
  local min = -10
  local max = 10
  return tostring(math.random(min, max))
end

function rand_bool()
  if math.random(2) == 1 then
    return "true"
  end
  return "false"
end

function rand_str()
  local chars = "abcdef"
  local len = math.random(1, 4)
  local out = ""
  for i = 1, len do
    local idx = math.random(#chars)
    out = out .. string.sub(chars, idx, idx)
  end
  return "'" .. out .. "'"
end

function pred_int()
  return pick(int_cols) .. " " .. pick(int_ops) .. " " .. rand_int()
end

function pred_bool()
  return pick(bool_cols) .. " " .. pick(eq_ops) .. " " .. rand_bool()
end

function pred_str()
  return pick(str_cols) .. " " .. pick(eq_ops) .. " " .. rand_str()
end

function pred_any()
  local choice = math.random(3)
  if choice == 1 then
    return pred_int()
  elseif choice == 2 then
    return pred_bool()
  end
  return pred_str()
end
}

query:
  select_stmt

select_stmt:
  SELECT select_cols FROM {{table_name}} WHERE expr

select_cols:
  *
  | {print(pick_cols(all_cols))}

expr:
  predicate
  | predicate logic_op expr

predicate:
  {print(pred_any())}

logic_op:
  {print(pick(logic_ops))}
