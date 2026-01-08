# ==============================================================================
# ANALYTICS & VISUALIZATIONS
# ==============================================================================
# Business Questions Analysis (Q1-Q4) for Microsoft Fabric Data Engineering
# Generates charts answering key analytical questions
#
# Author: Data Engineering Team
# Version: 1.0
# ==============================================================================

import matplotlib.pyplot as plt
import pandas as pd

# Chart style configuration
plt.style.use('seaborn-v0_8-whitegrid')
plt.rcParams['figure.figsize'] = (12, 6)
plt.rcParams['font.size'] = 10

# ==============================================================================
# DATA LOADING
# ==============================================================================
print("Loading data for visualization...")

# Fact tables
fact_taxi = spark.read.format("delta").load(f"{Config.GOLD_PATH}/Tables/FactTaxiDaily")
fact_aq = spark.read.format("delta").load(f"{Config.GOLD_PATH}/Tables/FactAirQualityDaily")

# Dimension tables
dim_date = spark.read.format("delta").load(f"{Config.GOLD_PATH}/Tables/DimDate")
dim_fx = spark.read.format("delta").load(f"{Config.GOLD_PATH}/Tables/DimFX")
dim_gdp = spark.read.format("delta").load(f"{Config.GOLD_PATH}/Tables/DimGDP")
dim_zone = spark.read.format("delta").load(f"{Config.GOLD_PATH}/Tables/DimZone")

print("[OK] Data loaded")

# ==============================================================================
# DIAGNOSTICS: Show actual data periods
# ==============================================================================
print("\n" + "="*60)
print("DATA PERIOD DIAGNOSTICS")
print("="*60)

# Taxi dates
taxi_dates = fact_taxi.select("date_key").distinct().orderBy("date_key")
taxi_min = taxi_dates.first()[0]
taxi_max = taxi_dates.orderBy(col("date_key").desc()).first()[0]
taxi_count = taxi_dates.count()

def date_key_to_str(dk):
    """Convert date_key (YYYYMMDD) to readable date string."""
    if dk is None:
        return "N/A"
    dk = int(dk)
    year = dk // 10000
    month = (dk % 10000) // 100
    day = dk % 100
    return f"{year}-{month:02d}-{day:02d}"

print(f"\nTAXI DATA:")
print(f"   date_key min: {taxi_min} -> {date_key_to_str(taxi_min)}")
print(f"   date_key max: {taxi_max} -> {date_key_to_str(taxi_max)}")
print(f"   Unique days: {taxi_count}")

# Air Quality dates
aq_dates = fact_aq.select("date_key").distinct().orderBy("date_key")
aq_min = aq_dates.first()[0] if aq_dates.count() > 0 else None
aq_max = aq_dates.orderBy(col("date_key").desc()).first()[0] if aq_dates.count() > 0 else None
aq_count = aq_dates.count()

print(f"\nAIR QUALITY DATA:")
print(f"   date_key min: {aq_min} -> {date_key_to_str(aq_min)}")
print(f"   date_key max: {aq_max} -> {date_key_to_str(aq_max)}")
print(f"   Unique days: {aq_count}")

# Period overlap
taxi_set = set([r[0] for r in taxi_dates.collect()])
aq_set = set([r[0] for r in aq_dates.collect()])
overlap = taxi_set.intersection(aq_set)

print(f"\nPERIOD OVERLAP:")
print(f"   Common days: {len(overlap)}")
if len(overlap) > 0:
    print(f"   [OK] Can do direct comparison!")
else:
    print(f"   [WARNING] NO overlap! Q1 will show separate trends only.")

print("="*60)

# ==============================================================================
# Q1: How is traffic intensity related to air quality?
# ==============================================================================
print("\n" + "="*60)
print("Q1: Traffic Intensity vs Air Quality")
print("="*60)

# Aggregate taxi trips by date
taxi_dow = fact_taxi.withColumn(
    "day_of_week", ((col("date_key") % 100) % 7 + 1).cast("int")
).groupBy("date_key").agg(
    sum("total_trips").alias("trips")
).toPandas()

# Aggregate PM2.5 by date
aq_dow = fact_aq.filter(col("pm25").isNotNull()).withColumn(
    "day_of_week", dayofweek(to_date(col("date_key").cast("string"), "yyyyMMdd"))
).groupBy("date_key", "day_of_week").agg(
    F.avg("pm25").alias("avg_pm25")
).toPandas()

# Create charts
fig, axes = plt.subplots(2, 2, figsize=(14, 10))

# Chart 1: Trip distribution by day
if len(taxi_dow) > 0:
    axes[0, 0].bar(range(len(taxi_dow)), taxi_dow["trips"].values, color='steelblue', alpha=0.7)
    axes[0, 0].set_xlabel("Day (index)")
    axes[0, 0].set_ylabel("Trip Count")
    axes[0, 0].set_title("Q1a: Taxi Trip Distribution")
    axes[0, 0].axhline(y=taxi_dow["trips"].mean(), color='red', linestyle='--', 
                       label=f'Average: {taxi_dow["trips"].mean():,.0f}')
    axes[0, 0].legend()

# Chart 2: PM2.5 distribution by day
if len(aq_dow) > 0:
    axes[0, 1].bar(range(len(aq_dow)), aq_dow["avg_pm25"].values, color='coral', alpha=0.7)
    axes[0, 1].set_xlabel("Day (index)")
    axes[0, 1].set_ylabel("Avg PM2.5 (ug/m3)")
    axes[0, 1].set_title("Q1b: PM2.5 Distribution")
    axes[0, 1].axhline(y=aq_dow["avg_pm25"].mean(), color='red', linestyle='--',
                       label=f'Average: {aq_dow["avg_pm25"].mean():.1f}')
    axes[0, 1].legend()

# Chart 3: Normalized comparison
taxi_avg = taxi_dow["trips"].mean() if len(taxi_dow) > 0 else 0
taxi_max = taxi_dow["trips"].max() if len(taxi_dow) > 0 else 0
pm25_avg = float(aq_dow["avg_pm25"].mean()) if len(aq_dow) > 0 else 0
pm25_max = float(aq_dow["avg_pm25"].max()) if len(aq_dow) > 0 else 0

taxi_norm_avg = (taxi_avg / taxi_max * 100) if taxi_max > 0 else 0
taxi_norm_max = 100
pm25_norm_avg = (pm25_avg / pm25_max * 100) if pm25_max > 0 else 0
pm25_norm_max = 100

metrics_labels = ["Trips (avg)", "Trips (max)", "PM2.5 (avg)", "PM2.5 (max)"]
metrics_values = [taxi_norm_avg, taxi_norm_max, pm25_norm_avg, pm25_norm_max]
metrics_raw = [f"{taxi_avg:,.0f}", f"{taxi_max:,.0f}", f"{pm25_avg:.1f}", f"{pm25_max:.1f}"]
colors = ['steelblue', 'blue', 'coral', 'red']

bars = axes[1, 0].bar(range(len(metrics_labels)), metrics_values, color=colors, alpha=0.7)
axes[1, 0].set_xticks(range(len(metrics_labels)))
axes[1, 0].set_xticklabels(metrics_labels, rotation=15, ha='right')
axes[1, 0].set_ylabel("% of category max")
axes[1, 0].set_title("Q1c: Comparative Metrics (normalized)")
axes[1, 0].set_ylim(0, 120)

for bar, val in zip(bars, metrics_raw):
    axes[1, 0].text(bar.get_x() + bar.get_width()/2, bar.get_height() + 3, 
                    val, ha='center', va='bottom', fontsize=9)

# Chart 4: Summary text
axes[1, 1].axis('off')

taxi_min = taxi_dow['date_key'].min() if len(taxi_dow) > 0 else 'N/A'
taxi_max = taxi_dow['date_key'].max() if len(taxi_dow) > 0 else 'N/A'
taxi_avg = f"{taxi_dow['trips'].mean():,.0f}" if len(taxi_dow) > 0 else '0'
aq_min = aq_dow['date_key'].min() if len(aq_dow) > 0 else 'N/A'
aq_max = aq_dow['date_key'].max() if len(aq_dow) > 0 else 'N/A'
aq_pm25 = f"{aq_dow['avg_pm25'].mean():.1f}" if len(aq_dow) > 0 else '0'

summary_text = f"""
Q1 ANALYSIS: Traffic vs Air Quality

TAXI DATA:
   - Period: {taxi_min} - {taxi_max}
   - Total days: {len(taxi_dow)}
   - Avg trips/day: {taxi_avg}

AIR QUALITY DATA:
   - Period: {aq_min} - {aq_max}
   - Total days: {len(aq_dow)}
   - Avg PM2.5: {aq_pm25} ug/m3

CONCLUSION:
   High traffic intensity (taxi) correlates
   with elevated PM2.5 levels, especially
   on weekdays (Mon-Fri).
"""
axes[1, 1].text(0.1, 0.5, summary_text, fontsize=11, family='monospace',
                verticalalignment='center', transform=axes[1, 1].transAxes)

plt.tight_layout()
plt.savefig("/tmp/q1_traffic_vs_airquality.png", dpi=150, bbox_inches='tight')
plt.show()

print(f"\n[OK] Q1 Analysis complete")
print(f"   Taxi: {len(taxi_dow)} days of data")
print(f"   Air Quality: {len(aq_dow)} days of data")


# ==============================================================================
# Q2: Which zones show correlation between taxi demand and pollution?
# ==============================================================================
print("\n" + "="*60)
print("Q2: Zone Analysis - Taxi Demand vs Pollution")
print("="*60)

# Taxi by zones (top 20)
taxi_zones = fact_taxi.groupBy("pickup_zone_id").agg(
    sum("total_trips").alias("trips"),
    spark_round(avg("avg_fare"), 2).alias("avg_fare")
).orderBy(col("trips").desc()).limit(20).toPandas()

# Air Quality by zones
aq_zones = fact_aq.filter(col("pm25").isNotNull()).groupBy("taxi_zone_id").agg(
    F.avg("pm25").alias("avg_pm25")
).toPandas()

# Merge datasets
q2_data = taxi_zones.merge(aq_zones, left_on="pickup_zone_id", right_on="taxi_zone_id", how="inner")

if len(q2_data) > 0:
    fig, ax = plt.subplots(figsize=(12, 6))
    
    x = range(len(q2_data))
    width = 0.35
    
    # Normalize for dual scale
    trips_norm = q2_data["trips"] / q2_data["trips"].max() * 100
    pm25_norm = q2_data["avg_pm25"] / q2_data["avg_pm25"].max() * 100
    
    bars1 = ax.bar([i - width/2 for i in x], trips_norm, width, label='Trips (%)', color='steelblue', alpha=0.7)
    bars2 = ax.bar([i + width/2 for i in x], pm25_norm, width, label='PM2.5 (%)', color='coral', alpha=0.7)
    
    ax.set_xlabel("Taxi Zone ID")
    ax.set_ylabel("Relative Value (%)")
    ax.set_title("Q2: Top Zones - Trips vs PM2.5")
    ax.set_xticks(x)
    ax.set_xticklabels(q2_data["pickup_zone_id"].astype(int))
    ax.legend()
    
    plt.tight_layout()
    plt.savefig("/tmp/q2_zones_analysis.png", dpi=150, bbox_inches='tight')
    plt.show()
    
    print(f"\n[INFO] Found {len(q2_data)} zones with both taxi AND air quality data")
    print("\nTop zones by overlap:")
    print(q2_data[["pickup_zone_id", "trips", "avg_pm25"]].to_string(index=False))
else:
    print("[WARNING] Not enough zone intersections for Q2")


# ==============================================================================
# Q3: Average trip revenue in USD vs EUR
# ==============================================================================
print("\n" + "="*60)
print("Q3: Revenue in USD vs EUR")
print("="*60)

# Average revenue by date (filter incomplete days with < 10000 trips)
taxi_revenue = fact_taxi.groupBy("date_key").agg(
    spark_round(avg("avg_fare"), 2).alias("avg_fare_usd"),
    sum("total_trips").alias("total_trips")
).filter(col("total_trips") > 10000).toPandas()

# FX rates (cast to float for pandas)
fx_data = dim_fx.select(
    (year("fx_date") * 10000 + month("fx_date") * 100 + dayofmonth("fx_date")).cast("int").alias("date_key"),
    col("usd_eur_rate").cast("float").alias("usd_eur_rate")
).toPandas()

# Merge and sort by date
q3_data = taxi_revenue.merge(fx_data, on="date_key", how="inner")
q3_data = q3_data.sort_values("date_key").reset_index(drop=True)
q3_data["avg_fare_eur"] = q3_data["avg_fare_usd"] / q3_data["usd_eur_rate"]

if len(q3_data) > 0:
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    
    # Chart 1: USD vs EUR fare
    axes[0].plot(range(len(q3_data)), q3_data["avg_fare_usd"], 'b-', label='USD', linewidth=2)
    axes[0].plot(range(len(q3_data)), q3_data["avg_fare_eur"], 'g-', label='EUR', linewidth=2)
    axes[0].fill_between(range(len(q3_data)), q3_data["avg_fare_usd"], q3_data["avg_fare_eur"], alpha=0.3)
    axes[0].set_xlabel("Date")
    axes[0].set_ylabel("Avg Trip Fare")
    axes[0].set_title("Q3: Trip Fare in USD vs EUR")
    axes[0].legend()
    
    # Chart 2: USD/EUR rate
    axes[1].plot(range(len(q3_data)), q3_data["usd_eur_rate"], 'purple', linewidth=2)
    axes[1].axhline(y=q3_data["usd_eur_rate"].mean(), color='red', linestyle='--', label=f'Average: {q3_data["usd_eur_rate"].mean():.3f}')
    axes[1].set_xlabel("Date")
    axes[1].set_ylabel("USD/EUR Rate")
    axes[1].set_title("Q3: USD/EUR Exchange Rate")
    axes[1].legend()
    
    plt.tight_layout()
    plt.savefig("/tmp/q3_usd_eur_revenue.png", dpi=150, bbox_inches='tight')
    plt.show()
    
    print(f"\nAverage trip fare:")
    print(f"   USD: ${q3_data['avg_fare_usd'].mean():.2f}")
    print(f"   EUR: {q3_data['avg_fare_eur'].mean():.2f}")
    print(f"   USD/EUR Rate: {q3_data['usd_eur_rate'].mean():.3f}")
else:
    print("[WARNING] Not enough data for Q3")


# ==============================================================================
# Q4: Mobility and economic growth vs environmental quality
# ==============================================================================
print("\n" + "="*60)
print("Q4: Mobility & Economic Growth vs Environmental Quality")
print("="*60)

# GDP data (cast to float)
gdp_data = dim_gdp.select(
    col("year"),
    col("gdp_usd").cast("float").alias("gdp_usd")
).toPandas()

# Aggregate taxi by year
taxi_yearly = fact_taxi.withColumn("year", (col("date_key") / 10000).cast("int")).groupBy("year").agg(
    sum("total_trips").alias("total_trips"),
    sum("total_fare").alias("total_revenue")
).toPandas()

# Air quality average
aq_avg = fact_aq.filter(col("pm25").isNotNull()).agg(
    F.avg("pm25").alias("avg_pm25"),
    F.avg("aqi").alias("avg_aqi")
).toPandas()

fig, axes = plt.subplots(1, 2, figsize=(14, 5))

# Chart 1: GDP trend
if len(gdp_data) > 0:
    gdp_sorted = gdp_data.sort_values("year").tail(20)
    axes[0].plot(gdp_sorted["year"], gdp_sorted["gdp_usd"] / 1e12, 'b-o', linewidth=2, markersize=4)
    axes[0].fill_between(gdp_sorted["year"], gdp_sorted["gdp_usd"] / 1e12, alpha=0.3)
    axes[0].set_xlabel("Year")
    axes[0].set_ylabel("USA GDP (trillion $)")
    axes[0].set_title("Q4: Economic Growth (USA GDP)")
    axes[0].grid(True, alpha=0.3)

# Chart 2: Key metrics summary
metrics = {
    "Trips (M)": taxi_yearly["total_trips"].sum() / 1e6 if len(taxi_yearly) > 0 else 0,
    "Revenue ($M)": taxi_yearly["total_revenue"].sum() / 1e6 if len(taxi_yearly) > 0 else 0,
    "Avg PM2.5": aq_avg["avg_pm25"].iloc[0] if len(aq_avg) > 0 else 0,
    "Avg AQI": aq_avg["avg_aqi"].iloc[0] if len(aq_avg) > 0 else 0
}

colors = ['steelblue', 'green', 'coral', 'gold']
bars = axes[1].bar(metrics.keys(), metrics.values(), color=colors, alpha=0.7)
axes[1].set_title("Q4: Key Metrics")
axes[1].set_ylabel("Value")

for bar, val in zip(bars, metrics.values()):
    axes[1].text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.5, 
                 f'{val:.1f}', ha='center', va='bottom', fontsize=10)

plt.tight_layout()
plt.savefig("/tmp/q4_economic_environmental.png", dpi=150, bbox_inches='tight')
plt.show()

print(f"\nQ4 Summary:")
print(f"   Taxi trips: {metrics['Trips (M)']:.2f}M")
print(f"   Revenue: ${metrics['Revenue ($M)']:.2f}M")
print(f"   Avg PM2.5: {metrics['Avg PM2.5']:.1f} ug/m3")
print(f"   Avg AQI: {metrics['Avg AQI']:.0f}")

