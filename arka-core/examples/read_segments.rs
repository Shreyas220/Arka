use arrow::ipc::reader::FileReader;
use std::fs::File;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("📖 Arka Segment Reader Example\n");

    let data_dir = std::path::PathBuf::from("./arka_data/segments");

    // Read all segment files
    let mut entries: Vec<_> = std::fs::read_dir(&data_dir)?
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("arrow"))
        .collect();

    entries.sort_by_key(|e| e.file_name());

    println!("Found {} segment files\n", entries.len());

    for entry in entries {
        let path = entry.path();
        let filename = path.file_name().unwrap().to_string_lossy();

        println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        println!("📄 Reading: {}", filename);
        println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

        let file = File::open(&path)?;
        let reader = FileReader::try_new(file, None)?;

        let schema = reader.schema();
        println!("\n📋 Schema:");
        for field in schema.fields() {
            println!("  {} ({})", field.name(), field.data_type());
        }

        println!("\n📊 Data:");
        for (batch_idx, batch_result) in reader.enumerate() {
            let batch = batch_result?;
            println!("  Batch {}: {} rows × {} columns", batch_idx, batch.num_rows(), batch.num_columns());

            // Print each row
            for row_idx in 0..batch.num_rows() {
                print!("    Row {}: ", row_idx);
                for col_idx in 0..batch.num_columns() {
                    let column = batch.column(col_idx);
                    let field = schema.field(col_idx);

                    use arrow::array::*;
                    let value = match field.data_type() {
                        arrow::datatypes::DataType::Utf8 => {
                            let array = column.as_any().downcast_ref::<StringArray>().unwrap();
                            array.value(row_idx).to_string()
                        }
                        arrow::datatypes::DataType::UInt8 => {
                            let array = column.as_any().downcast_ref::<UInt8Array>().unwrap();
                            array.value(row_idx).to_string()
                        }
                        arrow::datatypes::DataType::UInt64 => {
                            let array = column.as_any().downcast_ref::<UInt64Array>().unwrap();
                            array.value(row_idx).to_string()
                        }
                        arrow::datatypes::DataType::Timestamp(_, _) => {
                            let array = column.as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
                            array.value(row_idx).to_string()
                        }
                        _ => "?".to_string(),
                    };

                    print!("{}={}", field.name(), value);
                    if col_idx < batch.num_columns() - 1 {
                        print!(", ");
                    }
                }
                println!();
            }
        }
        println!();
    }

    println!("✨ Successfully read all segments!");

    Ok(())
}
