-- Target schema MUST pre-exist (replicare replicates data only, §7). Same shape
-- as the source table; replicare fills it.
CREATE TABLE public.orders (id int PRIMARY KEY, note text);
