INSERT INTO users (id, email, username, password_hash) VALUES
  ('00000000-0000-0000-0000-000000000001', 'alice@example.com', 'alice',
   '$2a$10$N9qo8uLOickgx2ZMRZo5i.ej6pHxUzH.fGs6Q.9aZrNnKjvT4SE3W'),
  ('00000000-0000-0000-0000-000000000002', 'bob@example.com', 'bob',
   '$2a$10$R9sE6gR1Z7p0W6w0BGx1leHw5Ej6cMlGZsxDnmZ3EZQZb2JjKNRNa')
ON CONFLICT DO NOTHING;
