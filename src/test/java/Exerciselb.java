class Exercise1b {
    public static void main(String [] args) {
        int x = 1;
        while ( x < 10 ) {
            x += 1;
            System.out.println("x value: " + x);
            if ( x > 3) {
                System.out.println("big x");
                //break;
            }
        }
    }
}
