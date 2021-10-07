use postgres::{error::Error, Client, NoTls};
use serde_json::{Number, Value};
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
#[structopt(name = "basic")]
struct Opt {
    /// Activate jsonb_only mode
    #[structopt(short = "j", long = "jsonb_only")]
    jsonb_only: bool,
}

#[derive(Debug)]
struct SaleWithProduct {
    category: String,
    name: String,
    quantity: f64,
    unit: String,
    date: i64,
}

fn create_db(opt: &Opt) -> Result<Client, Error> {
    let username = "chris";
    let password = "hello";
    let host = "localhost";
    let port = "5432";
    let database = "sports3d";

    let mut conn = Client::connect(
        &format!(
            "postgres://{}{}{}@{}{}{}{}{}",
            username,
            if password.is_empty() { "" } else { ":" },
            password,
            host,
            if port.is_empty() { "" } else { ":" },
            port,
            if database.is_empty() { "" } else { "/" },
            database
        ),
        NoTls,
    )?;

    let _ = conn.execute("DROP TABLE Sales", &[]);
    let _ = conn.execute("DROP TABLE Products", &[]);
    if opt.jsonb_only {
        conn.execute(
            "CREATE TABLE Products (
                id INTEGER PRIMARY KEY,
                category TEXT NOT NULL,
                name TEXT NOT NULL UNIQUE,
                sales jsonb null)",
            &[],
        )?;
    } else {
        conn.execute(
            "CREATE TABLE Products (
                id INTEGER PRIMARY KEY,
                category TEXT NOT NULL,
                name TEXT NOT NULL UNIQUE,
                sales_text text null,
                sales_jsonb jsonb null)",
            &[],
        )?;
    }
    conn.execute(
        "CREATE TABLE Sales (
            id TEXT PRIMARY KEY,
            product_id INTEGER NOT NULL REFERENCES Products,
            sale_date BIGINT NOT NULL,
            quantity DOUBLE PRECISION NOT NULL,
            unit TEXT NOT NULL)",
        &[],
    )?;
    Ok(conn)
}

fn grab_json_text_from_sample_file() -> String {
    // Code below based on sample from "Creative Projects for Rust Programmers"
    let input_path = "sales.json".to_string();

    let mut sales_and_products = {
        // Load the first file into a string.
        let sales_and_products_text = std::fs::read_to_string(&input_path).unwrap();

        // Parse the string into a dynamically-typed JSON structure.
        serde_json::from_str::<Value>(&sales_and_products_text).unwrap()
    };

    // Get the field of the structure
    // containing the weight of the sold oranges.
    if let Value::Number(n) = &sales_and_products["sales"][1]["quantity"] {
        // Increment it and store it back into the structure.
        sales_and_products["sales"][1]["quantity"] =
            Value::Number(Number::from_f64(n.as_f64().unwrap() + 1.5).unwrap());
    }

    // Save the JSON structure into the other file.
    serde_json::to_string(&sales_and_products).unwrap()
}

fn grab_json_from_sample_file() -> serde_json::Value {
    // Code below based on sample from "Creative Projects for Rust Programmers"
    let input_path = "sales.json".to_string();

    let mut sales_and_products = {
        // Load the first file into a string.
        let sales_and_products_text = std::fs::read_to_string(&input_path).unwrap();

        // Parse the string into a dynamically-typed JSON structure.
        serde_json::from_str::<Value>(&sales_and_products_text).unwrap()
    };

    // Get the field of the structure
    // containing the weight of the sold oranges.
    if let Value::Number(n) = &sales_and_products["sales"][1]["quantity"] {
        // Increment it and store it back into the structure.
        sales_and_products["sales"][1]["quantity"] =
            Value::Number(Number::from_f64(n.as_f64().unwrap() + 1.5).unwrap());
    }

    // Save the JSON structure into the other file.
    //serde_json::to_string(&sales_and_products).unwrap()
    sales_and_products
}

fn populate_db(conn: &mut Client, opt: &Opt) -> Result<(), Error> {
//    let sales_json = r#"{ "name" : "Julia" }"#;

    if opt.jsonb_only {
        let sales_json = grab_json_from_sample_file();
        conn.execute(
            "INSERT INTO Products (
                id, category, name, sales
                ) VALUES ($1, $2, $3, $4)",
            &[&1, &"fruit", &"pears", &sales_json],
        )?;
    } else {
        let sales_json_text = grab_json_text_from_sample_file();
        conn.execute(
            "INSERT INTO Products (
                id, category, name, sales_text
                ) VALUES ($1, $2, $3, $4)",
            &[&1, &"fruit", &"pears", &sales_json_text],
        )?;
        conn.execute(
            "UPDATE Products 
                SET sales_jsonb = cast (sales_text as jsonb)
              WHERE sales_jsonb is null", &[]
        )?;
    }

    conn.execute(
        "INSERT INTO Sales (
            id, product_id, sale_date, quantity, unit
            ) VALUES ($1, $2, $3, $4, $5)",
                &[&"2020-183",          // id
                &1,                     // product_id
                &1_234_567_890_i64,     // sale_date,
                &7.439,                 // quanity
                &"Kg"                   // unit
        ],
    )?;
    Ok(())
}

fn print_db(conn: &mut Client) -> Result<(), Error> {
    for row in &conn.query(
        "SELECT p.name, s.unit, s.quantity, s.sale_date
        FROM Sales s
        LEFT JOIN Products p
        ON p.id = s.product_id
        ORDER BY s.sale_date",
        &[],
    )? {
        let sale_with_product = SaleWithProduct {
            category: "".to_string(),
            name: row.get(0),
            quantity: row.get(2),
            unit: row.get(1),
            date: row.get(3),
        };
        println!(
            "At instant {}, {} {} of {} were sold.",
            sale_with_product.date,
            sale_with_product.quantity,
            sale_with_product.unit,
            sale_with_product.name
        );
    }
    Ok(())
}

fn main() -> Result<(), Error> {
    let opt = Opt::from_args();
    println!("opt = {:?}", opt);

    let mut conn = create_db(&opt)?;
    populate_db(&mut conn, &opt)?;
    print_db(&mut conn)?;
    Ok(())
}
