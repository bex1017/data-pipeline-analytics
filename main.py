import extract
import transform
import load
import analyze


def run_pipeline():
    print("\n=== Starting Weather Data Pipeline ===\n")

    # Step 1: Extract
    print("Extracting data...")
    df = extract.extract(past_days=3, forecast_days=7)
    print(f"Extracted {len(df)} rows.\n")

    # Step 2: Transform
    print("Transforming data...")
    df = transform.transform(df, forecast_days=7)
    print(f"Transformed dataset has {len(df)} rows.\n")

    # Step 3: Load
    print("Loading data into database...")
    load.load(df)
    print("Data loaded successfully.\n")

    # Step 4: Analyze
    print("Running analysis...")
    analyze.analyze()
    print("\nAnalysis complete.\n")

    print("=== Pipeline finished successfully ===\n")


if __name__ == "__main__":
    run_pipeline()