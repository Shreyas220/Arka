#!/usr/bin/env python3
"""
Arka Flight Client Example (Python)

This example demonstrates how to:
1. Connect to Arka Flight server
2. Create a table via Flight actions
3. Write data using bidirectional streaming (DoExchange)
4. Receive durability acknowledgments in real-time

Prerequisites:
    pip install pyarrow pandas

Usage:
    # Start server first:
    cargo run --example flight_server

    # Then run this client:
    python examples/flight_client.py
"""

import pyarrow as pa
import pyarrow.flight as flight
import json
import time
from datetime import datetime, timezone

def main():
    print("🚀 Connecting to Arka Flight Server...\n")

    # 1. Connect to server
    client = flight.FlightClient("grpc://localhost:8815")
    print("✅ Connected to grpc://localhost:8815\n")

    # 2. List available tables
    print("📋 Listing tables...")
    try:
        result = client.do_action(
            flight.Action("ListTables", b"")
        )
        for response in result:
            tables = json.loads(response.body.to_pybytes().decode('utf-8'))
            print(f"   Available tables: {tables}")
    except Exception as e:
        print(f"   Note: {e}")

    print()

    # 3. Create a new table (optional - already exists from server)
    print("📊 Creating table 'test_table' (if not exists)...")
    create_table_request = {
        "name": "test_table",
        "schema_json": json.dumps({
            "fields": [
                {"name": "id", "type": "Int64", "nullable": False},
                {"name": "message", "type": "Utf8", "nullable": False},
                {"name": "timestamp", "type": "Timestamp", "nullable": False},
            ]
        }),
        "primary_key": "id",
        "table_type": "primary_key"
    }

    try:
        result = client.do_action(
            flight.Action(
                "CreateTable",
                json.dumps(create_table_request).encode('utf-8')
            )
        )
        for response in result:
            print(f"   {response.body.to_pybytes().decode('utf-8')}")
    except Exception as e:
        print(f"   Table might already exist: {e}")

    print()

    # 4. Prepare data to write
    print("✍️  Preparing data to write to 'users' table...")

    # Create sample data
    data = {
        "id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
        "email": [
            "alice@example.com",
            "bob@example.com",
            "charlie@example.com",
            "diana@example.com",
            "eve@example.com"
        ],
        "signup_date": [
            datetime.now(timezone.utc) for _ in range(5)
        ],
        "age": [25, 30, 28, 35, 32],
    }

    schema = pa.schema([
        ("id", pa.int64()),
        ("name", pa.utf8()),
        ("email", pa.utf8()),
        ("signup_date", pa.timestamp('us', tz='UTC')),
        ("age", pa.int32()),
    ])

    batch = pa.record_batch(data, schema=schema)
    print(f"   Created batch with {batch.num_rows} rows\n")

    # 5. Write data using DoExchange (bidirectional streaming)
    print("🔄 Starting bidirectional streaming write...")
    print("   Sending data and receiving acks simultaneously...\n")

    # Prepare write request metadata
    write_request = {
        "table_name": "users",
        "requested_durability": "disk",  # Request disk-level acks
    }

    # Create flight descriptor
    descriptor = flight.FlightDescriptor.for_path("users")

    # Start DoExchange stream
    try:
        # Create writer and reader streams
        writer, reader = client.do_exchange(descriptor)

        # Send write request metadata in first message
        metadata = json.dumps(write_request).encode('utf-8')

        # Create flight data with batch and metadata
        data_generator = flight.RecordBatchStream(pa.Table.from_batches([batch]))

        # Write batch
        writer.begin(schema, options=flight.FlightCallOptions(write_options={'metadata': metadata}))
        writer.write_batch(batch)
        writer.done_writing()

        # Read acknowledgments
        print("📨 Receiving acknowledgments:")
        ack_count = 0
        for chunk in reader:
            if chunk.app_metadata:
                try:
                    ack = json.loads(chunk.app_metadata.decode('utf-8'))
                    ack_count += 1
                    print(f"   ACK #{ack_count}:")
                    print(f"      LSN: {ack['lsn']}")
                    print(f"      Durability: {ack['durability_level']}")
                    print(f"      Timestamp: {ack['timestamp_us']}")
                    if 'error' in ack and ack['error']:
                        print(f"      ⚠️  Error: {ack['error']}")
                    else:
                        print(f"      ✅ Success")
                    print()
                except json.JSONDecodeError as e:
                    print(f"   Could not decode ack: {e}")

        if ack_count == 0:
            print("   ⚠️  No acknowledgments received (might need to check server implementation)")
        else:
            print(f"✅ Received {ack_count} acknowledgment(s)\n")

    except Exception as e:
        print(f"❌ Error during streaming: {e}")
        import traceback
        traceback.print_exc()
        return

    # 6. Verify write by getting schema (demonstrates table exists)
    print("🔍 Verifying table schema...")
    try:
        schema_result = client.get_schema(descriptor)
        schema_from_server = schema_result.schema
        print(f"   ✅ Schema verified: {len(schema_from_server)} fields")
        for field in schema_from_server:
            print(f"      - {field.name}: {field.type}")
    except Exception as e:
        print(f"   Error getting schema: {e}")

    print("\n✨ Example complete!")
    print("   Data written with LSN tracking and durability guarantees")
    print("   Check server logs for processing details\n")

if __name__ == "__main__":
    main()
