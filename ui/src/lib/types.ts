export type Dataset = {
  dataset_id: string;
  name: string;
  source: string;
  row_count: number;
  size_bytes: number;
  last_profiled_at?: string | null;
};

export type Column = {
  dataset_id: string;
  column_name: string;
  data_type: string;
  pii_flag: boolean;
  null_ratio?: number | null;
  distinct_ratio?: number | null;
};

export type LineageEdge = {
  src_dataset_id: string;
  dst_dataset_id: string;
  transform_type: string;
  updated_at: string;
};

