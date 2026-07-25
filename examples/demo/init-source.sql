-- Source schema + seed data. replicare installs its capture triggers here.
CREATE TABLE public.orders (id int PRIMARY KEY, note text);
INSERT INTO public.orders SELECT g, 'order '||g FROM generate_series(1, 100) g;
