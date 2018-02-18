package net.twentyonesolutions.m2pg;

public interface IProgress {

    void progress(Status status);

    class Status {

        static final Status EMPTY = new Status("", 0, 0);

        final String name;
        final long row;
        final long rowCount;

        public Status(String name, long row, long rowCount) {
            this.name = name;
            this.row = row;
            this.rowCount = rowCount;
        }

        @Override
        public String toString() {
            return String.format("%s %,d/%,d %.1f%%", name, row, rowCount, 100.0 * row / rowCount);
        }
    }
}
