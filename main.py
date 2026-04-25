from fastapi import FastAPI, HTTPException
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg
import uvicorn

app = FastAPI(title="User Analytics API", description="FastAPI + PySpark REST API for user data")

spark = SparkSession.builder \
 .appName("UserAnalytics") \
 .master("local[*]") \
 .getOrCreate()

df = spark.read.csv("users.csv", header=True, inferSchema=True)

@app.get("/")
def read_root():
    return {"message": "User Analytics API running. Go to /docs for Swagger UI"}

@app.get("/users")
def get_all_users():
    users = df.toPandas().to_dict(orient="records")
    return {"total": len(users), "users": users}

@app.get("/users/{user_id}")
def get_user_by_id(user_id: int):
    result = df.filter(col("id") == user_id).toPandas().to_dict(orient="records")
    if not result:
        raise HTTPException(status_code=404, detail="User not found")
    return result[0]

@app.get("/users/active")
def get_active_users():
    active_df = df.filter(col("active") == True)
    active_users = active_df.toPandas().to_dict(orient="records")
    return {"count": len(active_users), "users": active_users}

@app.get("/analytics/city")
def get_users_by_city():
    city_count = df.groupBy("city").agg(count("*").alias("user_count"))
    result = city_count.toPandas().to_dict(orient="records")
    return {"city_analytics": result}

@app.get("/analytics/avg-age")
def get_average_age():
    avg_age = df.filter(col("active") == True).agg(avg("age").alias("average_age"))
    result = avg_age.toPandas().to_dict(orient="records")[0]
    return {"average_age_active_users": round(result["average_age"], 2)}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
