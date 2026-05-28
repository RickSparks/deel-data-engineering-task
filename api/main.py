#################################################
#
# 1. Import Libraries
#
from fastapi import FastAPI


#################################################
#
# 2. Import my utils
#
from api.routers import analytics



#################################################
#
# 3. Builds app
#
app = FastAPI(
    title="Analytics API",
    description="Provides real-time analytics powered by a CDC streaming pipeline.",
    version="1.0.0",
)



#################################################
#
# 4. Includes the router I built
#
app.include_router(analytics.router)



#################################################
#
# 5. Health check
#
@app.get("/health")
def health_check():
    return {"status": "ok"}