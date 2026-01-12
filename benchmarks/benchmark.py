import time
import random
import psycopg2
import csv
import threading
from concurrent.futures import ThreadPoolExecutor

PG_DSN = "host=localhost dbname=testdb user=admin password=password port=5432"
NUM_THREADS = 15  # Simulate 15 concurrent users

def setup_fresh_state():
    print("--- Initializing Advanced Benchmark State ---")
    print("Connecting to PostgreSQL...")
    conn = psycopg2.connect(PG_DSN)
    conn.autocommit = True
    cur = conn.cursor()
    
    print("Dropping existing table 'employees'...")
    cur.execute("DROP TABLE IF EXISTS employees CASCADE;")
    
    print("Creating table 'employees'...")
    cur.execute("""
        CREATE TABLE employees (
            id SERIAL PRIMARY KEY,
            employee_id INT,
            name TEXT,
            department TEXT,
            salary INT,
            status TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    
    print(f"Seeding 300,000 rows...")
    cur.execute("""
        INSERT INTO employees (employee_id, name, department, salary, status)
        SELECT 
            (random() * 900000 + 100000)::int,
            'User_' || i || '_' || md5(random()::text),
            (ARRAY['HR', 'IT', 'Sales', 'Marketing', 'Engineering', 'Operations', 'Legal'])[floor(random() * 7 + 1)],
            (random() * 100000 + 30000)::int,
            (ARRAY['Active', 'Inactive', 'OnLeave', 'Terminated'])[floor(random() * 4 + 1)]
        FROM generate_series(1, 300000) s(i);
    """)
    
    print("Running ANALYZE...")
    cur.execute("ANALYZE employees;")
    cur.close()
    conn.close()
    print("--- Initialization Complete ---")

def worker_task(stop_event, results, write_ratio, start_time):
    conn = psycopg2.connect(PG_DSN)
    conn.autocommit = True
    cur = conn.cursor()
    
    read_cutoff = 1.0 - write_ratio
    
    while not stop_event.is_set():
        q_start = time.time()
        q_type, target = "READ", "unknown"
        
        try:
            if random.random() < read_cutoff:
                # --- READ WORKLOAD (Varied types) ---
                r = random.random()
                
                # 1. Range Query on Salary (Classic B-Tree candidate)
                if r < 0.25:
                    low = random.randint(30000, 120000)
                    cur.execute(f"SELECT count(*) FROM employees WHERE salary BETWEEN {low} AND {low + 5000}")
                    target = "salary_range"
                    
                # 2. Point Lookup on Employee ID (Unique/High Cardinality)
                elif r < 0.50:
                    eid = random.randint(100000, 900000)
                    cur.execute(f"SELECT name FROM employees WHERE employee_id = {eid}")
                    target = "empid_point"
                    
                # 3. Text Pattern Search (LIKE 'User_100%') - Tests text indexing
                elif r < 0.75:
                    # Generate a prefix that matches some rows
                    prefix = f"User_{random.randint(1, 20000)}"
                    cur.execute(f"SELECT count(*) FROM employees WHERE name LIKE '{prefix}%'")
                    target = "name_pattern"
                    
                # 4. Filter on Low Cardinality Column (Department)
                # Actuator should ideally NOT index this
                else:
                    dept = random.choice(['HR', 'IT', 'Sales', 'Marketing', 'Engineering'])
                    cur.execute(f"SELECT count(*) FROM employees WHERE department = '{dept}'")
                    target = "dept_equality"
                    
            else:
                # --- WRITE WORKLOAD (Churn) ---
                q_type = "WRITE"
                r = random.random()
                
                # Insert
                if r < 0.7:
                    cur.execute(f"INSERT INTO employees (employee_id, name, salary, department, status) VALUES ({random.randint(900000, 999999)}, 'BenchUser', {random.randint(30000, 90000)}, 'Test', 'Active')")
                    target = "insert"
                # Update (Generates dead tuples)
                elif r < 0.9:
                    eid = random.randint(100000, 900000)
                    cur.execute(f"UPDATE employees SET salary = salary + 50 WHERE employee_id = {eid}")
                    target = "update"
                # Delete
                else:
                    eid = random.randint(100000, 900000)
                    cur.execute(f"DELETE FROM employees WHERE employee_id = {eid}")
                    target = "delete"

            latency = (time.time() - q_start) * 1000
            results.append({
                "elapsed_sec": round(time.time() - start_time, 2),
                "type": q_type,
                "target": target,
                "latency_ms": round(latency, 2)
            })
            
        except Exception:
            # Swallow connection errors during shutdown/overload
            pass
            
        # Micro-sleep to prevent total lockup of local docker
        time.sleep(0.001)

    cur.close()
    conn.close()

def run_workload(duration_sec, write_ratio, run_name):
    print(f"--- Starting Concurrent Workload: {run_name} ({duration_sec}s) ---")
    print(f"Threads: {NUM_THREADS} | Write Ratio: {write_ratio:.0%}")
    
    results = []
    stop_event = threading.Event()
    start_time = time.time()
    
    with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        futures = [
            executor.submit(worker_task, stop_event, results, write_ratio, start_time)
            for _ in range(NUM_THREADS)
        ]
        
        # Monitoring loop
        while (time.time() - start_time) < duration_sec:
            elapsed = int(time.time() - start_time)
            if elapsed > 0 and elapsed % 30 == 0:
                print(f"Status: {elapsed}s | Queries: {len(results)} | Rate: {int(len(results)/elapsed)} qps")
            time.sleep(1)
            
        print("Stopping threads...")
        stop_event.set()
        for f in futures:
            f.result()

    print(f"--- Workload {run_name} Finished. Total queries: {len(results)} ---")
    return results

if __name__ == "__main__":
    scenarios = [
        (0.50, '50w_50r'),  # 50% writes, 50% reads
    ]
    
    try:
        # One time DB Setup
        setup_fresh_state()
        
        for write_ratio, name in scenarios:
            # Reset DB between runs to keep tests independent
            if name != scenarios[0][1]:
                setup_fresh_state()

            data = run_workload(600, write_ratio, name)  # 10 minutes each
            
            filename = f'benchmark_results_{name}.csv'
            print(f"Saving results to {filename}...")
            with open(filename, 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=["elapsed_sec", "type", "target", "latency_ms"])
                writer.writeheader()
                writer.writerows(data)
                
        print("All benchmark scenarios complete.")
    except Exception as e:
        print(f"Error: {e}")