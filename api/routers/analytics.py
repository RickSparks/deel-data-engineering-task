#################################################
#
# 1. Import Libraries
#
from fastapi import APIRouter, Query, HTTPException




#################################################
#
# 2. Import my utils
#
from database import get_db



#################################################
#
# 3. Builds the router
#
router = APIRouter(prefix="/analytics", tags=["Analytics"])




#################################################
#
# 4. Builds the 4 requested Endpoints
#

# 4.1 Endpoint 1:
#   - GET /analytics/orders?status=open
#   - returns # orders grouped by delivery date and status
#   - Business Q: "Number of open orders by delivery_date and status"
@router.get("/orders")
def get_orders_by_delivery_date_and_status(
    status: str = Query(default="open", description="Filter by order status")
):
    
    with get_db() as cur:

        # Execute query
        cur.execute("""
            SELECT
                f.delivery_date,
                f.order_status as status,
                COUNT(DISTINCT f.order_id) as total_orders
            FROM analytics.fact_order_items f
            WHERE LOWER(f.order_status) = LOWER(%(status)s)
            GROUP BY f.delivery_date, f.order_status
            ORDER BY f.delivery_date ASC
        """, {"status": status})

        # Fetch rows
        rows = cur.fetchall()

    # If there's no rows
    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"No orders found with status '{status}'"
        )

    return {
        "status_filter": status,
        "total_records": len(rows),
        "data": [dict(row) for row in rows]
    }


# 4.2 Endpoint 2:
#   - GET /analytics/orders/top?limit=3
#   - returns top N delivery dates with the most open orders
#   - Business Q: "Top 3 delivery dates with more open orders"
@router.get("/orders/top")
def get_top_delivery_dates(
    limit:  int = Query(default=3, ge=1, le=100, description="Number of results to return"),
    status: str = Query(default="open", description="Filter by order status")
):
    
    with get_db() as cur:

        # Execute query
        cur.execute("""
            SELECT
                delivery_date,
                COUNT(DISTINCT order_id)    AS total_open_orders
            FROM analytics.fact_order_items
            WHERE LOWER(order_status) = LOWER(%(status)s)
              AND delivery_date IS NOT NULL
            GROUP BY delivery_date
            ORDER BY total_open_orders DESC
            LIMIT %(limit)s
        """, {"limit": limit, "status": status})

        rows = cur.fetchall()

    # If there's no rows
    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"No orders found with status '{status}'"
        )

    return {"status_filter": status, "limit": limit, "data": [dict(row) for row in rows]}



# 4.3 Endpoint 3:
#   - GET /analytics/orders/product
#   - returns number of pending items grouped by product_id.
#   - Business Q: "Number of open pending items by product_id"
@router.get("/orders/product")
def get_pending_items_by_product(
    status: str = Query(default="pending", description="Filter by order status")
):
    
    # Execute query  
    with get_db() as cur:
        cur.execute("""
            SELECT
                f.product_id,
                p.product_name,
                SUM(f.quantity)             AS total_pending_quantity,
                COUNT(DISTINCT f.order_id)  AS total_orders
            FROM analytics.fact_order_items f
            LEFT JOIN analytics.dim_products p
                ON f.product_id = p.product_id
               AND p.is_current = TRUE
            WHERE LOWER(f.order_status) = LOWER(%(status)s)
            GROUP BY f.product_id, p.product_name
            ORDER BY total_pending_quantity DESC
        """, {"status": status})

        # Fetch rows
        rows = cur.fetchall()

    # If there's no rows
    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"No orders found with status '{status}'"
        )

    return {"status_filter": status, "total_products": len(rows), "data": [dict(row) for row in rows]}



# 4.4 Endpoint 4:
#   - GET /analytics/orders/customers?status=open&limit=3
#   - returns top N customers with the most orders of a given status.
#   - Business Q: "Top 3 customers with more pending orders"
@router.get("/orders/customers")
def get_top_customers_by_order_status(
    status: str  = Query(default="open",  description="Filter by order status"),
    limit:  int  = Query(default=3, ge=1, le=100, description="Number of results")
):
    
    with get_db() as cur:

        # Execute query        
        cur.execute("""
            SELECT
                f.customer_id,
                c.customer_name,
                COUNT(DISTINCT f.order_id)  AS total_orders
            FROM analytics.fact_order_items f
            LEFT JOIN analytics.dim_customers c
                ON f.customer_id = c.customer_id
            WHERE LOWER(f.order_status) = LOWER(%(status)s)
            GROUP BY f.customer_id, c.customer_name
            ORDER BY total_orders DESC
            LIMIT %(limit)s
        """, {"status": status, "limit": limit})

        # Fetch rows
        rows = cur.fetchall()

    # If there's no rows
    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"No customers found with status '{status}'"
        )

    return {
        "status_filter": status,
        "limit": limit,
        "data": [dict(row) for row in rows]
    }