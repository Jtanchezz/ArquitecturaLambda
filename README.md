# ArquitecturaLambda

{
  "event_id": "string (uuid hex)",
  "event_time": "string ISO-8601 UTC (ej: 2026-02-25T18:04:22Z)",
  "event_name": "string enum",
  "user_id": "string",
  "session_id": "string",
  "device": "string (mobile|desktop|tablet)",
  "currency": "string (ej: USD)",

  "product_id": "string|null",
  "quantity": "number|null",
  "price": "number|null",

  "cart_id": "string",
  "checkout_id": "string|null",
  "order_id": "string|null",

  "cart_items_qty": "number|null",
  "cart_value": "number|null",
  "revenue": "number|null",

  "page_url": "string|null",
  "referrer": "string|null",

  "items": "array|null"
}
