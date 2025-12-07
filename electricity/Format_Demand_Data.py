import pandas as pd
from datetime import datetime

df = pd.read_csv('city_hall_electricty_usage.csv')

df['DateTime_Measured'] = pd.to_datetime(df['DateTime_Measured'])
df['date'] = df['DateTime_Measured'].dt.date

aggregated = df.groupby('date')['Total_Demand_KW'].sum().reset_index()
aggregated.columns = ['date', 'total_electricity']

# Convert from kilowatts to watts (1 kW = 1000 W)
aggregated['total_electricity'] = aggregated['total_electricity'] * 1000

aggregated.to_csv('aggregated_electricity.csv', index=False)

print(f"Aggregated data written to aggregated_electricity.csv")
print(f"Total dates: {len(aggregated)}")
print(f"\nFirst few rows:")
print(aggregated.head())

agg_df = pd.read_csv('aggregated_electricity.csv')

agg_df['date'] = pd.to_datetime(agg_df['date'])
agg_df['day_of_week'] = agg_df['date'].dt.day_name()

avg_by_day = agg_df.groupby('day_of_week')['total_electricity'].mean().reset_index()
avg_by_day.columns = ['day_of_week', 'average_total_electricity']

day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
avg_by_day['day_of_week'] = pd.Categorical(avg_by_day['day_of_week'], categories=day_order, ordered=True)
avg_by_day = avg_by_day.sort_values('day_of_week')

avg_by_day.to_csv('demand.csv', index=False)

print(f"\n{'='*60}")
print("Average total electricity per day of the week:")
print(f"{'='*60}")
print(avg_by_day.to_string(index=False))
print(f"\nResults written to demand.csv")

