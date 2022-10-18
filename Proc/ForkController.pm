package Proc::ForkController;
use vars qw($ppid $workers $async $alarm $child);

use POSIX qw(WNOHANG SIG_BLOCK SIG_UNBLOCK);

sub STOP(){'STOP'}
sub WAIT(){'WAIT'}
sub SPWN(){'SPWN'}
sub WORKERS(){35}

############################
### Instance constructor ###
############################
sub new{
  my $invocant = shift;
  my $class = ref $invocant || $invocant;
  my $sigset = POSIX::SigSet->new;
  $sigset->fillset;
  $ppid = $$;
  my $self = {
    _code => sub {
      my $param = shift;
      ( srand $$, sleep int rand 7 );
      print "demo $param executed by pid ${$}$/"},
    _status => STOP,
    _gotoSTOP => 1,
    _async => $async,
    _alarm => !$alarm || $] < 5.007003 ? 0 : 1,
    _child => !$child || $] < 5.007003 ? 0 : 1,
    _sigset => $sigset,
    _pids => {},
    _exitcode => {},
    _lastSpawned => -1,
    _params => [qw(job1 job2 job3 job4 job5)]};
  bless $self => $class;
  $self->workers($ENV{WORKERS} || $workers || WORKERS);
  $self;
}

########################
### Instance methods ###
########################
sub start{
  my $self = shift;
  $$self{_async} = shift if $#_>=0;
  $$self{_gotoSTOP} = 0;
  $self->workers(WORKERS) if !defined $$self{_workers};
  $self->_child;
  $self->_alarm;
  $self->resume;
}
sub stop{
  my $self = shift;
  $$self{_gotoSTOP} = 1;
  return if $$self{_async};
  $self->resume;
}
sub sync{
  my $self = shift;
  $$self{_async} = 0;
}
sub resume{
  my ($self,$int) = @_;
  while($self->_refreshStatus && $$self{_status} ne STOP){
    $self->_spawn if $$self{_status} eq SPWN;
    last if $int || $$self{_async};
    select(undef,undef,undef,0.05)}
}

###########################
### Instance properties ###
###########################
sub status{
  my $self = shift;
  $$self{_status};
}
sub exitcode{
  my $self = shift;
  %{$$self{_exitcode}};
}
sub pids{
  my $self = shift;
  my $pids = $$self{_pids};
  foreach my $pid (sort {$$pids{$a}<=>$$pids{$b}} keys %$pids){
    local $?;
    if(waitpid $pid,POSIX::WNOHANG > 0){
      $$self{_exitcode}{join'_',delete $$pids{$pid},time,$pid} = $?>>8}}
  $$self{_pids};
}
sub code{
  my $self = shift;
  if(scalar @_){
    if(local $_ = shift){
      if(!ref){
        my ( $lvl => $caller ) = ( 0 );
        $lvl++ while ( $caller = (caller $lvl)[0] ) eq __PACKAGE__;
        local *alias = "${caller}::$_";
        $_ = *alias{CODE}}
      $$self{_code} = $_ if defined && ref eq 'CODE'}}
  $$self{_code};
}
sub params{
  my $self = shift;
  if(scalar @_ && $$self{_status} eq STOP){
    $$self{_params} = [@_]}
  $$self{_params};
}
sub workers{
  my $self = shift;
  if($_[0] && $_[0] =~ /^\d+$/){
    $$self{_workers} = $_[0] if $_[0] <= 100}
  $$self{_workers};
}

########################
### Internal methods ###
########################
sub _sigBlock{
  my $self = shift;
  POSIX::sigprocmask POSIX::SIG_BLOCK,$$self{_sigset};
}
sub _sigUnblock{
  my $self = shift;
  POSIX::sigprocmask POSIX::SIG_UNBLOCK,$$self{_sigset};
}
sub _alarm{
  my $self = shift;
  return if defined $$self{_alarm} && $$self{_alarm}==0;
  if(!exists $$self{_SIG} || !exists $$self{_SIG}{ALRM}){
    $$self{_SIG}{ALRM} = $SIG{ALRM};
    $SIG{ALRM} = sub {
      if($$self{_status} eq STOP){
        $SIG{ALRM} = delete $$self{_SIG}{ALRM};
        alarm delete $$self{_SIG}{ALRMrem};
      }else{
        alarm 3;
        $self->resume(shift)}};
    $$self{_SIG}{ALRMrem} = alarm 3}
}
sub _child{
  my $self = shift;
  return if defined $$self{_child} && $$self{_child}==0;
  if(!exists $$self{_SIG} || !exists $$self{_SIG}{CHLD}){
    $$self{_SIG}{CHLD} = $SIG{CHLD};
    $SIG{CHLD} = sub {
      if($$self{_status} eq STOP){
        $SIG{CHLD} = delete $$self{_SIG}{CHLD};
      }else{
        $self->pids;
        $self->resume(shift)}}}
}
sub _spawn{
  my $self = shift;
  $self->_sigBlock;
  while($$self{_status} eq SPWN){
    my $params = $$self{_params};
    if(exists $$params[1+$$self{_lastSpawned}]){
      my $nextParams = $$params[1+$$self{_lastSpawned}];
      my $code = $$self{_code};
      if($code && ref $code eq 'CODE'){
        defined(my $fork = fork) or die "Cannot fork: $!$/";
        $$self{_pids}{$fork} = time;
        $$self{_lastSpawned}++;
        if(!$fork){
          $$self{_status} = STOP;
          $SIG{ALRM}->() if $$self{_alarm};
          $SIG{CHLD}->() if $$self{_child};
          $self->_sigUnblock;
          %$self = ();
          $code->($nextParams);
          exit }}}
    $self->_refreshStatus}
  $self->_sigUnblock;
}
sub _refreshStatus{
  my $self = shift;
  return unless $self && ref $self eq __PACKAGE__;
  my $params = $$self{_params};
  my $pen = $#$params - $$self{_lastSpawned};
  my $stp = $$self{_gotoSTOP};
  my $pids = $$self{_child} ? $$self{_pids} : $self->pids;
  my $run = keys %$pids;
  my $avl = $$self{_workers} - $run;
  return if scalar grep !defined,$avl,$pen,$run,$stp;
  $$self{_status} = SPWN if  $avl &&  $pen &&          !$stp ;
  $$self{_status} = STOP if  $avl && !$pen && !$run          ;
  $$self{_status} = STOP if  $avl &&  $pen && !$run &&  $stp ;
  $$self{_status} = WAIT if !$avl                            ;
  $$self{_status} = WAIT if  $avl && !$pen &&  $run          ;
  $$self{_status} = WAIT if  $avl &&  $pen &&  $run &&  $stp ;
  $SIG{ALRM}->() if $$self{_status}eq STOP && $$self{_alarm}
                 && $$self{_SIG}  && $$self{_SIG}{ALRM}      ;
  $SIG{CHLD}->() if $$self{_status}eq STOP && $$self{_child}
                 && $$self{_SIG}  && $$self{_SIG}{CHLD};1    ;
}
#sub DESTROY{return unless $$==$ppid;&sync;&stop}
1;
