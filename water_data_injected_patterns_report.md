# Understanding the Injected Patterns in the Water Data

## 1. Dataset Overview

The dataset simulates a water distribution system over 25 days, from January 1, 2026, across six isolated district networks containing 600 meters. The network is simulated using EPANET 2.2 through WNTR, with hydraulic calculations performed at one-minute resolution.

The generated data contains two main streams:
- **District sensor readings:** inlet flow and pressure measurements recorded every minute.
- **Meter readings:** customer consumption volumes recorded every 15 minutes.

The dataset includes normal demand variation, realistic hydraulic effects, injected water-system scenarios, and deliberately introduced data-quality faults.

## 2. Normal Operating Patterns

Normal demand is not constant throughout the day. Each meter receives a baseline demand based on its customer class, with residential and small-commercial customers using different demand profiles.

The simulation incorporates several sources of variation:
- **Daily patterns:** demand increases during morning and evening periods and decreases during quieter hours.
- **Weekly patterns:** demand changes according to the day of the week.
- **Friday effects:** demand decreases around Friday prayer time and subsequently rebounds.
- **Random variation:** a randomly generated multiplier is sampled once every 15 minutes and held constant throughout that period.

The random multiplier follows a normal distribution with a mean of 1.0 and a standard deviation of 0.035, clipped to the range from 0.85 to 1.15. The random seed is fixed to make the simulation reproducible.

Residential demand has a baseline mean of 0.0000105 m³/s, while small-commercial demand has a baseline mean of 0.000021 m³/s. The small-commercial profile also uses a multiplier of 1.15.

These patterns matter because a sudden increase or decrease in consumption does not necessarily indicate a fault. A detection system must distinguish unusual behavior from normal demand cycles.

## 3. Injected Water-System Scenarios

The scenarios are injected into the hydraulic simulation itself. Consequently, their effects appear in the generated flow, pressure, and consumption measurements rather than being represented by artificial labels in the public data.

### 3.1 Background Leakage — D006

A small leak is active throughout the simulation in district D006.

    C_background = 0.00001

The leak remains active for the entire 25-day period. Its actual flow depends on the pressure at the leak location.

The purpose of this scenario is to represent persistent background water loss. Because the leak is continuous and relatively small, its effects may be difficult to distinguish from ordinary variation using a single measurement.

### 3.2 Sudden Burst — D001

A burst is introduced in district D001 using an emitter coefficient of:

    C_burst = 0.00012

The leak is active for two hours and then closed.

    q_leak = C × P^γ

Here, q_leak is the leak flow rate, C is the emitter coefficient, P is the pressure at the leak location, and γ is the emitter exponent.

The coefficient controls the relationship between pressure and leak flow. The actual flow is determined by the hydraulic simulation and is not simply equal to the coefficient.

A burst can produce a sudden increase in water loss and changes in pressure or inlet flow. However, the magnitude and visibility of these effects depend on the simulated network conditions.

### 3.3 Gradual Leak — D002

The gradual-leak scenario introduces leakage through three branches, activated in stages.

    C_total = 0.00008
    C_stage = C_total / 3

The activation schedule is:
- First branch: simulation start.
- Second branch: four hours after the start.
- Third branch: eight hours after the start.

All branches are closed at the end of the configured incident interval.

As additional branches become active, the total leakage can increase progressively. This scenario represents a developing leak rather than a single abrupt failure.

    q_total_leak = sum(q_leak_i for each active branch)

Here, the sum includes the flows through all active leak branches. Because each branch responds to its local hydraulic conditions, dividing the coefficient does not guarantee that the resulting flows will be identical.

### 3.4 Supply Pressure Disturbance — D003

The source head is reduced from 45 metres to 15 metres for two hours.

    H_normal = 45 m
    H_disturbed = 45 - 30 = 15 m
    Percentage reduction = ((45 - 15) / 45) × 100 = 66.7%

This scenario represents a supply-side disturbance rather than a leak. The reduced source head can affect pressure throughout the district and may reduce the amount of demand that the network can deliver.

A pressure drop alone should therefore not be interpreted as proof of leakage. The detector needs to consider the relationship between pressure, inlet flow, and customer consumption.

### 3.5 Demand Surge — D001

During the demand-surge interval, the requested demand of every meter assigned to district D001 is multiplied by 1.6.

    d_new(m,t) = 1.6 × d_original(m,t)

Here, d_original(m,t) is the original requested demand for meter m at time t, and d_new(m,t) is the modified requested demand.

This represents a 60% increase in requested demand for two hours.

The increase in actual delivered consumption may be smaller than 60% because the simulation uses pressure-dependent demand. If pressure becomes insufficient, customers may receive less than they requested.

After the hydraulic simulation, district-level delivered demand is distributed among its meters in proportion to their requested demands. This preserves the relative allocation of consumption among customers while reflecting the total amount actually delivered by the simulated network.

## 4. Pressure-Dependent Demand

The hydraulic model uses pressure-dependent demand (PDD), with the following parameters:

    P_min = 5 m
    P_required = 20 m

The configured demand exponent is 0.5.

Below the minimum pressure, the model does not supply the full requested demand. Between the minimum and required pressures, delivered demand increases with pressure according to the PDD relationship. At or above the required pressure, the requested demand can be fully delivered.

This behavior is important for interpreting the injected scenarios. A supply disturbance or leak may reduce pressure, which can then reduce customer consumption even when requested demand remains unchanged or increases.

Consequently, customer consumption, pressure, and inlet flow should be interpreted together rather than as independent signals.

## 5. How the Meter Readings Are Calculated

The hydraulic simulation produces delivered customer flow at one-minute resolution, but the meter stream records consumption over 15-minute intervals.

    V(m,k) = 60 × sum(q(m,t), for t = k through k+14)

Here, V(m,k) is the total consumption volume in m³, q(m,t) is the delivered flow rate in m³/s, and k is the starting minute of the interval. The factor 60 converts the flow rate into volume for each minute.

The resulting meter event represents the accumulated consumption over the full 15-minute interval, not an instantaneous flow measurement.

This difference in granularity matters when correlating meter readings with district sensor readings. A single 15-minute meter volume should not be compared directly with a single one-minute inlet flow rate.

## 6. Water-Balance Validation

The simulation validates the physical water balance for every district and every simulated minute.

    Q_in ≈ Q_customers + Q_leaks

When the flow rates are converted into volumes over one minute:

    V_in ≈ V_customers + V_leaks

The validation code calculates the difference as:

    ΔV = (Q_in - Q_customers - Q_leaks) × 60

The permitted absolute difference is 0.0001 m³ per minute.

This validation uses the internal simulation results before the public data-quality faults are injected. Therefore, a physically balanced simulation can still produce public records containing missing values or duplicate events.

For downstream processing, comparisons between district and meter streams must account for their different time resolutions. Meter volumes should be aggregated or aligned to the same interval before using them in a water-balance calculation.

## 7. Injected Data-Quality Faults

The data-generation pipeline introduces three deliberate faults into the delivery data. These faults are separate from the hydraulic scenarios.

### 7.1 Missing Meter Consumption

Every 25,000th meter input record has its `consumed_volume_m3` value replaced with `null`.

This tests whether downstream systems can detect missing consumption measurements without treating them as zero consumption. A missing value and a genuine zero-volume reading have different meanings. Replacing a null with zero without preserving a quality indicator could distort historical baselines, aggregation results, and anomaly detection.

### 7.2 Missing District Pressure

Every 5,000th district input record has its `pressure_m_h2o` value replaced with `null`.

This tests whether processing pipelines correctly handle incomplete sensor readings. A missing pressure value should not automatically be interpreted as a pressure drop. The system should preserve the missing-data condition and avoid using the record as if it contained a valid measurement.

### 7.3 Duplicate Meter Events

Every 25,000th meter input record is duplicated identically, including its event ID.

This tests whether streaming and batch pipelines can identify repeated events and prevent duplicate consumption from being counted twice.

Using `event_id` as a Kafka message key does not, by itself, guarantee deduplication. The consumer must explicitly implement an appropriate deduplication strategy.

The fault rules operate on the serialized input-record order rather than on a particular timestamp or district. Therefore, the injected faults should not be assumed to occur at regular chronological intervals or to be uniformly distributed across all districts.

The faulted records are written to the delivery data, while the public clean files remain unchanged. The fault-generation process also produces a separate fault-truth file describing the injected faults.

## 8. Timestamp and Stream Alignment

The district and meter streams use different timestamp conventions.

- **District sensor records:** `event_time_utc` represents the beginning of the one-minute measurement interval; `available_time_utc` represents the end of that interval.
- **Meter records:** `event_time_utc` represents the end of the 15-minute consumption interval; `available_time_utc` also represents the end of that interval.

All stored event timestamps use UTC. Local Egyptian time is used when generating demand profiles, but the output timestamps remain in UTC.

These conventions mean that exact timestamp equality is not a reliable way to join the two streams. The processing logic should instead use interval-aware alignment, windowing, or aggregation.

The distinction between event time and availability time is also important for streaming. Event time describes the measurement interval, while availability time describes when the completed measurement becomes available to the pipeline.

## 9. Implications for Flink and Spark

### Flink: Streaming Detection

Flink should focus on processing incoming events correctly and detecting changes as data arrives.

- Handle event time and availability time correctly.
- Align one-minute district measurements with 15-minute meter volumes.
- Detect missing values and duplicate event IDs.
- Maintain windowed state for flow, pressure, and consumption trends.
- Identify sudden changes and persistent deviations from expected behavior.
- Avoid automatically classifying every pressure drop or consumption increase as leakage.

The different injected scenarios are useful for testing whether online logic can distinguish a burst, a gradual leak, a supply disturbance, and a demand surge using their combined effects.

### Spark: Historical Analysis and Batch Processing

Spark can analyze larger historical windows and compare observed behavior with established baselines.

- Build historical demand baselines by meter, district, and time of day.
- Compare normal patterns with unusual periods.
- Calculate aggregated water-balance indicators.
- Measure the frequency and impact of missing or duplicated records.
- Evaluate detection results against the scenario and fault truth files.

Historical analysis can help determine whether a deviation is unusual for the relevant time and customer group rather than merely unusual compared with a global average.

## 10. Ground Truth and Evaluation

The pipeline maintains separate truth files for the injected scenarios and data-quality faults. The scenario-truth file describes the configured incidents, while the fault-truth file records the injected data-quality faults. These files support evaluation and help determine whether the processing systems detect the intended behavior.

They should not be used as detector inputs or as a source of hard-coded scenario timestamps. The detector should operate on the public measurements and their observable features, with the truth files reserved for evaluating its results.

It is also important to distinguish configured scenario metadata from measured outcomes. A scenario definition describes what the generator was instructed to inject; it does not, by itself, establish the magnitude of the resulting change in flow, pressure, or consumption.

## 11. What the Experiments Should Demonstrate

The dataset is designed to test more than whether a pipeline can identify a predefined scenario name. A useful evaluation should establish whether the processing systems can:

- Detect sudden and gradual changes in water-system behavior.
- Distinguish supply disturbances and demand surges from potential leakage.
- Handle ordinary daily and weekly demand variation without excessive false alarms.
- Preserve correctness when measurements are missing or events are duplicated.
- Correlate streams with different time resolutions and timestamp conventions.
- Maintain physically meaningful water-balance calculations.
- Evaluate detections against ground truth without leaking that information into the detection logic.

The key principle is that **an anomaly is an observed deviation, while identifying its cause requires evidence from multiple signals and the surrounding context**. The injected scenarios provide controlled examples for evaluating whether the system can make that distinction.
