use std::collections::{BTreeMap, BTreeSet};

use prost::Message as _;
use serde_json::{json, Value};

use crate::proto::xstore as pb;

pub const MAX_POINTS: usize = 20_000;

pub fn sys_info_to_json(rep: &pb::SysInfoRep) -> Value {
    let dbs: Vec<Value> = rep
        .db_info
        .iter()
        .map(|d| {
            let files = d
                .files
                .iter()
                .map(|f| {
                    json!({
                        "name": f.file_name,
                        "actualBytes": f.actual_size_bytes,
                        "apparentBytes": f.apparent_size_bytes,
                    })
                })
                .collect::<Vec<_>>();
            json!({
                "name": d.db_name,
                "resolution": d.db_resolution,
                "sizeBytes": d.total_size_in_byte,
                "rowLength": d.row_length,
                "files": files,
            })
        })
        .collect();
    json!({
        "serverAddr": rep.server_addr,
        "threadCount": rep.thread_count,
        "dbs": dbs,
    })
}

fn fnum(v: f64) -> Value {
    serde_json::Number::from_f64(v)
        .map(Value::Number)
        .unwrap_or(Value::Null)
}

pub fn fdata_to_json(f: Option<&pb::FData>) -> Value {
    use pb::f_data::OneOfData as D;
    let Some(f) = f else {
        return Value::Null;
    };
    let Some(d) = f.one_of_data.as_ref() else {
        return Value::Null;
    };
    Some(match d {
        D::DoubleData(v) => fnum(*v),
        D::FloatData(v) => fnum(*v as f64),
        D::Int32Data(v) => Value::Number((*v).into()),
        D::Int64Data(v) => Value::Number((*v).into()),
        D::Uint32Data(v) => Value::Number((*v).into()),
        D::Uint64Data(v) => Value::Number((*v).into()),
        D::Sint32Data(v) => Value::Number((*v).into()),
        D::Sint64Data(v) => Value::Number((*v).into()),
        D::Fixed32Data(v) => Value::Number((*v).into()),
        D::Fixed64Data(v) => Value::Number((*v).into()),
        D::Sfixed32Data(v) => Value::Number((*v).into()),
        D::Sfixed64Data(v) => Value::Number((*v).into()),
        D::BoolData(v) => Value::from(*v),
        D::StringData(s) => Value::String(s.clone()),
        D::BytesData(b) => Value::String(format!("<{} bytes>", b.len())),
    })
    .unwrap_or(Value::Null)
}

pub fn range_query_to_json(rep: &pb::RangeQueryRep, raw: bool) -> Value {
    let rows = &rep.query_response;
    let total = rows.len();

    // Plot-mode downsampling: keep every row that carries data (XStore DBs
    // are often sparse -- a year of random writes is mostly holes), then fill
    // the remaining budget with stride-sampled hole rows so the timeline
    // shape survives. Only if data rows alone exceed the budget do we fall
    // back to plain stride sampling of everything. raw (table mode) skips
    // this entirely — dense data (e.g. 1-second) must not skip in the table.
    let mut idx: BTreeSet<usize> = BTreeSet::new();
    for (i, r) in rows.iter().enumerate() {
        if r.data.as_ref().map(|d| !d.field_data.is_empty()).unwrap_or(false) {
            idx.insert(i);
        }
    }
    let data_rows = idx.len();

    // Sizing diagnostic (handoff doc §3): exact per-row serialized size via
    // encoded_len() on the RowData the server returned — the same wire format
    // XStore wrote, so no formula reimplementation. Stride-capped so a 4M-row
    // chunk costs ~10k encodes, not 4M.
    const SIZE_SAMPLE_CAP: usize = 10_000;
    let stride = (idx.len() / SIZE_SAMPLE_CAP).max(1);
    // slot_max = the PROPER slot for the data. XStore row slots are aligned
    // to a power of two (8B = 64-bit word size), so the correct size rounds
    // the serialized row up to that alignment: (row + (align - 1)) & ~(align
    // - 1). A 55B row needs 56B — NOT the next power of two (64B): power-of-
    // two rounding over-allocates (a 1408B row would "need" 2048B, 46%
    // wasted). Max padding waste is (align - 1) = 7B per row.
    //
    // Note sz_max here is drawn from rows the server already returned, i.e.
    // already-stored rows — XStore rejects (never truncates) any insert
    // whose serialized size exceeds rowLength, so sz_max <= rowLength is a
    // server-enforced invariant, not a heuristic. slot_max can only exceed
    // rowLength when rowLength itself isn't a multiple of SLOT_ALIGN (still
    // >= sz_max) — this is an alignment mismatch, never a "data doesn't fit"
    // condition; genuinely-oversized rows never appear in this sample at
    // all, since they were never accepted in the first place.
    const SLOT_ALIGN: u64 = 8;
    let mut sz_min: Option<usize> = None;
    let mut sz_max: usize = 0;
    let mut sz_sum: u64 = 0;
    let mut slot_max: usize = 0;
    let mut sz_n = 0;
    for (k, &i) in idx.iter().enumerate() {
        if k % stride != 0 {
            continue;
        }
        let Some(d) = rows[i].data.as_ref() else {
            continue;
        };
        let sz = d.encoded_len() as u64;
        sz_min = Some(sz_min.map_or(sz as usize, |m| m.min(sz as usize)));
        sz_max = sz_max.max(sz as usize);
        sz_sum += sz;
        slot_max = slot_max.max(((sz + SLOT_ALIGN - 1) & !(SLOT_ALIGN - 1)) as usize);
        sz_n += 1;
    }
    let row_sizes = if sz_n > 0 {
        json!({
            "sampled": sz_n,
            "min": sz_min,
            "max": sz_max,
            "avg": sz_sum as f64 / sz_n as f64,
            "formulaMax": slot_max,
        })
    } else {
        Value::Null
    };

    let sampled: Vec<usize> = if raw {
        (0..total).collect()
    } else if data_rows <= MAX_POINTS {
        let step = total.div_ceil(MAX_POINTS).max(1);
        let mut i = 0;
        while idx.len() < MAX_POINTS && i < total {
            idx.insert(i);
            i += step;
        }
        idx.into_iter().collect()
    } else {
        let step = total.div_ceil(MAX_POINTS).max(1);
        (0..total).step_by(step).collect()
    };

    let ncols = sampled
        .iter()
        .map(|&i| rows[i].data.as_ref().map(|d| d.field_data.len()).unwrap_or(0))
        .max()
        .unwrap_or(0);

    // Every column array is built to exactly sampled.len() so the frontend
    // stays aligned even when rows are short (a row need not populate every
    // column) or a cell is absent -> Value::Null keeps the slot.
    let mut timestamps: Vec<u64> = Vec::with_capacity(sampled.len());
    let mut cols: BTreeMap<usize, Vec<Value>> = (0..ncols)
        .map(|i| (i, Vec::with_capacity(sampled.len())))
        .collect();
    for &i in &sampled {
        let r = &rows[i];
        timestamps.push(r.timestamp);
        let row = r.data.as_ref();
        for c in 0..ncols {
            let f = row.and_then(|d| d.field_data.get(c));
            cols.get_mut(&c).unwrap().push(fdata_to_json(f));
        }
    }

    json!({
        "rowsReturned": total,
        "rowsPlotted": timestamps.len(),
        "dataRows": data_rows,
        "columns": ncols,
        "timestamps": timestamps,
        "cols": cols,
        "rowSizes": row_sizes,
        "status": rep.query_status.clone().unwrap_or_default(),
    })
}

pub fn unary_query_to_json(rep: &pb::UnaryQueryRep) -> Value {
    let mut cols: BTreeMap<usize, Value> = BTreeMap::new();
    if let Some(row) = &rep.data {
        for (i, f) in row.field_data.iter().enumerate() {
            cols.insert(i, fdata_to_json(Some(f)));
        }
    }
    json!({
        "timestamp": rep.timestamp,
        "cols": cols,
        "status": rep.query_status.clone().unwrap_or_default(),
    })
}
