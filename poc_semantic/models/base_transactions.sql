select
  t.txn_id,
  t.amount,
  t.amount as gmv,
  t.txn_ts,
  t.customer_id,
  t.merchant_id
from public.transactions t
