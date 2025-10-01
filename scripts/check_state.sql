SELECT now() AS ts, current_database() db, current_user u;

-- what exists?
SELECT n.nspname AS schema, c.relname AS name, c.relkind
FROM pg_class c
JOIN pg_namespace n ON n.oid=c.relnamespace
WHERE n.nspname='public'
  AND c.relname IN ('raw_inspire','parcels','parcel_1acre','os_open_uprn',
                    'parcel_uprn_base','parcel_uprn_stats','parcel_uprn_clusters',
                    'parcel_features','target_cohorts')
ORDER BY c.relname;

-- safe counts if present
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='raw_inspire') THEN
    RAISE NOTICE 'raw_inspire rows: %', (SELECT COUNT(*) FROM public.raw_inspire);
  END IF;
  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='parcels') THEN
    RAISE NOTICE 'parcels rows: %', (SELECT COUNT(*) FROM public.parcels);
  END IF;
  IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='os_open_uprn') THEN
    RAISE NOTICE 'os_open_uprn rows: %', (SELECT COUNT(*) FROM public.os_open_uprn);
  END IF;
  IF EXISTS (SELECT 1 FROM information_schema.views WHERE table_schema='public' AND table_name='parcel_1acre') THEN
    RAISE NOTICE 'parcel_1acre rows: %', (SELECT COUNT(*) FROM public.parcel_1acre);
  END IF;
END $$;
