package net.twentyonesolutions.m2pg;

public class Column implements Comparable<Column> {

    final String name;
    final String type;
    final String defaultVal;
    final boolean isComputed;
    final boolean isIdentity;
    final boolean isNullable;
    final int maxLength;
    final int numericPrecision;
    final int pos;


    public Column(String name, String type, int pos, boolean isNullable, int maxLength, int numericPrecision, boolean isIdentity, boolean isComputed, String defaultVal) {
        this.name = name;
        this.type = type.toUpperCase();
        this.pos = pos;
        this.isNullable = isNullable || isComputed;     // make computed columns nullable
        this.maxLength = maxLength;
        this.numericPrecision = numericPrecision;
        this.isIdentity = isIdentity;
        this.isComputed = isComputed;
        this.defaultVal = (defaultVal == null) ? "" : defaultVal;
    }


    public boolean isChar(){
        return this.type.contains("CHAR");
    }

    /**
     * Compares this object with the specified object for order.  Returns a
     * negative integer, zero, or a positive integer as this object is less
     * than, equal to, or greater than the specified object.
     * <p>
     * <p>The implementor must ensure
     * {@code sgn(x.compareTo(y)) == -sgn(y.compareTo(x))}
     * for all {@code x} and {@code y}.  (This
     * implies that {@code x.compareTo(y)} must throw an exception iff
     * {@code y.compareTo(x)} throws an exception.)
     * <p>
     * <p>The implementor must also ensure that the relation is transitive:
     * {@code (x.compareTo(y) > 0 && y.compareTo(z) > 0)} implies
     * {@code x.compareTo(z) > 0}.
     * <p>
     * <p>Finally, the implementor must ensure that {@code x.compareTo(y)==0}
     * implies that {@code sgn(x.compareTo(z)) == sgn(y.compareTo(z))}, for
     * all {@code z}.
     * <p>
     * <p>It is strongly recommended, but <i>not</i> strictly required that
     * {@code (x.compareTo(y)==0) == (x.equals(y))}.  Generally speaking, any
     * class that implements the {@code Comparable} interface and violates
     * this condition should clearly indicate this fact.  The recommended
     * language is "Note: this class has a natural ordering that is
     * inconsistent with equals."
     * <p>
     * <p>In the foregoing description, the notation
     * {@code sgn(}<i>expression</i>{@code )} designates the mathematical
     * <i>signum</i> function, which is defined to return one of {@code -1},
     * {@code 0}, or {@code 1} according to whether the value of
     * <i>expression</i> is negative, zero, or positive, respectively.
     *
     * @param o the object to be compared.
     * @return a negative integer, zero, or a positive integer as this object
     * is less than, equal to, or greater than the specified object.
     * @throws NullPointerException if the specified object is null
     * @throws ClassCastException   if the specified object's type prevents it
     *                              from being compared to this object.
     */
    @Override
    public int compareTo(Column o) {

        int c = Integer.compare(this.pos, o.pos);
        if (c != 0)
            return c;

        c = this.name.compareToIgnoreCase(o.name);

        return c;
    }

    /**
     * Returns a string representation of the object. In general, the
     * {@code toString} method returns a string that
     * "textually represents" this object. The result should
     * be a concise but informative representation that is easy for a
     * person to read.
     * It is recommended that all subclasses override this method.
     * <p>
     * The {@code toString} method for class {@code Object}
     * returns a string consisting of the name of the class of which the
     * object is an instance, the at-sign character `{@code @}', and
     * the unsigned hexadecimal representation of the hash code of the
     * object. In other words, this method returns a string equal to the
     * value of:
     * <blockquote>
     * <pre>
     * getClass().getName() + '@' + Integer.toHexString(hashCode())
     * </pre></blockquote>
     *
     * @return a string representation of the object.
     */
    @Override
    public String toString() {

        StringBuilder sb = new StringBuilder(64);
        sb.append(this.name);
        sb.append(' ');
        sb.append(this.type);

        if (this.maxLength > 0){
            sb.append('(').append(this.maxLength).append(')');
        }
//        else if (this.numericPrecision > 0){
//            sb.append('(').append(this.numericPrecision).append(')');
//        }

        if (!this.isNullable)
            sb.append(" NOT NULL");

        return sb.toString();
    }
}
