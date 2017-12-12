import sqlite3
import os
import logging

log = logging.getLogger(__name__)
log.addHandler(logging.StreamHandler())
log.setLevel(logging.INFO)


def do_migration(db_dir):
    log.info("Doing the migration")
    db_refactor(db_dir)
    log.info("Migration succeeded")


def db_refactor(db_dir):
    new_db_path = os.path.join(db_dir, "lbrynet.sqlite")

    name_metadata_path = os.path.join(db_dir, "blockchainname.db")
    lbryfile_info_db_path = os.path.join(db_dir, 'lbryfile_info.db')
    blobs_db_path = os.path.join(db_dir, 'blobs.db')

    name_metadata_db = sqlite3.connect(name_metadata_path)
    lbryfile_db = sqlite3.connect(lbryfile_info_db_path)
    new_db = sqlite3.connect(new_db_path)
    blobs_db = sqlite3.connect(blobs_db_path)

    name_metadata_cursor = name_metadata_db.cursor()
    lbryfile_cursor = lbryfile_db.cursor()
    new_db_cursor = new_db.cursor()
    blobs_db_cursor = blobs_db.cursor()

    new_db.executescript("""
            pragma foreign_keys = on;
            pragma journal_mode=WAL
    
            create table blob (
                blob_hash char(96) primary key not null,
                blob_length integer not null,
                next_announce_time integer not null,
                should_announce integer not null default 0
            );
            
            create table stream (
                stream_hash char(96) not null primary key,
                sd_hash char(96) not null,
                stream_key text not null,
                stream_name text not null,
                suggested_filename text not null,
                foreign key(sd_hash) references blob(blob_hash)
            );
            
            create table stream_blob (
                stream_hash char(96) not null,
                blob_hash char(96),
                position integer not null,
                iv char(32) not null,
                primary key (stream_hash, blob_hash),
                foreign key(stream_hash) references stream(stream_hash),
                foreign key (blob_hash) references blob(blob_hash)
            );
            
            create table claim (
                claim_outpoint text not null primary key,
                claim_id char(64) not null,
                claim_name text not null,
                amount real,
                channel_outpoint text
            );

            create table claim_metadata (
                claim_outpoint text not null,
                serialized_metadata blob not null,
                foreign key(claim_outpoint) references claim(claim_outpoint)
            );

            create table claim_txi (
                txo text not null,
                txi text not null,
                primary key (txo, txi),
                foreign key(txo) references claim(claim_outpoint)
            );
            
            create table file (
                stream_hash text primary key not null,
                claim_outpoint text not null,
                file_name text not null,
                blob_data_rate real not null,
                status text not null,
                foreign key(stream_hash) references stream(stream_hash),
                foreign key(claim_outpoint) references claim(claim_outpoint)
            );
    """)
    new_db.commit()

    stream_descriptors = lbryfile_cursor.execute("select * from lbry_file_descriptors").fetchall()
    file_infos = {x[1]: {'sd_hash': x[0]} for x in stream_descriptors}
    for (rate, status, stream_hash) in lbryfile_cursor.execute("select "
                                                               "blob_data_rate, status, stream_hash "
                                                               "from lbry_file_options").fetchall():
        file_infos[stream_hash]['status'] = status
        file_infos[stream_hash]['rate'] = rate
    streams = lbryfile_cursor.execute("select rowid, * from lbry_files").fetchall()
    for s in streams:
        file_infos[s[1]]['rowid'] = s[0]
        file_infos[s[1]]['file_name'] = s[4]
    stream_blobs = lbryfile_cursor.execute("select stream_hash, blob_hash, position, iv "
                                           "from lbry_file_blobs").fetchall()

    for stream_hash in file_infos.keys():
        txid, nout = lbryfile_cursor.execute("select txid, n from lbry_file_metadata "
                                             "where lbry_file=?",
                                             (file_infos[stream_hash]['rowid'],)).fetchone()
        if txid is None or nout is None:
            log.warning("Missing outpoint, cannot migrate stream %s", stream_hash)
            del file_infos[stream_hash]
            continue
        claim_info = name_metadata_cursor.execute("select claimId, name from claim_ids "
                                     "where txid=? and n=?", (txid, nout)).fetchone()
        if not claim_info:
            log.warning("Missing claim id, cannot migrate stream %s", stream_hash)
            del file_infos[stream_hash]
            continue
        claim_id, name = claim_info
        file_infos[stream_hash]['claim_name'] = name
        file_infos[stream_hash]['claim_id'] = claim_id
        file_infos[stream_hash]['outpoint'] = "%s:%i" % (txid, nout)

    blobs = blobs_db_cursor.execute("select * from blobs").fetchall()

    for (blob_hash, blob_length, _, next_announce_time, should_announce) in blobs:
        new_db.execute("insert into blob values (?, ?, ?, ?)",
                       (blob_hash, blob_length, int(next_announce_time), should_announce))
    new_db.commit()

    for s in streams:
        if s[1] in file_infos:
            new_db_cursor.execute("insert into stream values (?, ?, ?, ?, ?)",
                                 (s[1], file_infos[s[1]]['sd_hash'], s[2], s[3], s[4]))
    new_db.commit()

    for (stream_hash, blob_hash, position, iv) in stream_blobs:
        if stream_hash in file_infos:
            new_db_cursor.execute("insert into stream_blob values (?, ?, ?, ?)",
                                  (stream_hash, blob_hash, position, iv))

    for stream_hash, file_info in file_infos.iteritems():
        new_db_cursor.execute("insert into claim values (?, ?, ?, ?, ?)",
                              (file_info['outpoint'], file_info['claim_id'],
                               file_info['claim_name'], 0, None))
        new_db.commit()
        new_db_cursor.execute("insert into file values (?, ?, ?, ?, ?)",
                              (stream_hash, file_info['outpoint'], file_info['file_name'],
                               file_info['rate'], file_info['status']))
        new_db.commit()

    new_db.commit()
    new_db.close()
    blobs_db.close()
    lbryfile_db.close()
    name_metadata_db.close()

    os.remove(lbryfile_info_db_path)
    os.remove(name_metadata_path)
