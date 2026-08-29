use redb::{Database, Error, ReadableDatabase, TableDefinition};

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

pub fn retrieve_message(db: &Database, seq_num: u64) -> Result<Option<Vec<u8>>, Error> {
    let txn = db.begin_read()?;
    let row =txn.open_table(MESSAGES)?.get(seq_num)?.map(|row| row.value().to_vec());
    Ok(row)
}
