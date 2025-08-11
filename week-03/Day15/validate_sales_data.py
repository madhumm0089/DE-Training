import great_expectations as ge
from great_expectations.data_context import DataContext

# Step 1: Load GE context
context = DataContext()

# Step 2: Read the CSV into a GE dataframe
df = ge.read_csv("data/sales.csv")

# Step 3: Use in-memory batch for validation
batch = context.get_batch({
    "dataset": df,
    "expectation_suite_name": "my_suite"
})

# Step 4: Add Expectations
batch.expect_column_values_to_be_between("quantity", 1, 100)
batch.expect_column_values_to_be_between("price_per_unit", 0, 100)
batch.expect_column_values_to_not_be_null("order_id")
batch.expect_column_values_to_not_be_null("order_date")

# Step 5: Save the expectations
batch.save_expectation_suite(discard_failed_expectations=False)

# Step 6: Build Data Docs
context.build_data_docs()

print("✅ Validation complete. Open: great_expectations/uncommitted/data_docs/local_site/index.html")
