use redb::{Database, Error, TableDefinition};

const MESSAGES: TableDefinition<u64, &[u8]> = TableDefinition::new("messages");

pub fn db_client(path: &str) -> Result<Database, Error> {
    let db = Database::create(path)?;
    Ok(db)
}

pub fn store_message(db: &Database, seq_num: u64, message: &[u8]) -> Result<(), Error> {
    let txn = db.begin_write()?;
    {
        txn.open_table(MESSAGES)?.insert(seq_num, message)?;
    }
    txn.commit()?;
    Ok(())
}
