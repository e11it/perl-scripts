package Sema4;

use threads::shared;

sub new {
    my $class = shift;
    my $val : shared = @_ ? shift : 1;

    # Workaround because of memory leak
    return bless \\$val, $class;
}

sub down {
    my $s = shift;
    # Double dereferencing
    $s = $$s;
    lock($$s);
    my $inc = @_ ? shift : 1;
    cond_wait $$s until $$s >= $inc;
    $$s -= $inc;
}

sub up {
    my $s = shift;
    # Double dereferencing
    $s = $$s;
    lock($$s);
    my $inc = @_ ? shift : 1;
    ($$s += $inc) > 0 and cond_broadcast $$s;
}

sub get {
    my $s = shift;
    # Double dereferencing
    $s = $$s;
    lock($$s);
    return $$s;
}

1;
