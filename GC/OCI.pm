package GC::OCI;
#use JSON::PP;
use Storable ();
use JSON::XS;
use Proc::ForkController;
use strict;

our $VERSION = '0.1';

our @fields_vgbackup = qw(unique-size-in-gbs unique-size-in-mbs type source-type lifecycle-state time-created expiration-time);
sub new{
  bless {}
}
sub compartments{
  my $self = shift;
  return $$self{_compartments} if $$self{_compartments};
  $$self{_compartments} = [];
  warn localtime()." |  fetching compartments$/";
  local $_ = qx/oci iam compartment list --all --compartment-id-in-subtree 1/;
  die "Cannot fetch compartments" if $?;
  warn localtime()." |  fetching compartments OK$/";
  my $json = decode_json $_;
  $$self{_compartments} = [map {$$_{id}} @{$$json{data}}];
}
sub avdomains{
  my $self = shift;
  return $$self{_avdomains} if $$self{_avdomains};
  $$self{_avdomains} = [];
  warn localtime()." |  fetching availability domains$/";
  local $_ = qx/oci iam availability-domain list --all/;
  die "Cannot fetch availability domains" if $?;
  warn localtime()." |  fetching availability domains OK$/";
  my $json = decode_json $_;
  $$self{_avdomains} = [map {$$_{name}} @{$$json{data}}];
}
sub instances_vgbackup{
  my $self = shift;
  return $$self{_instances_vgbackup} if $$self{_instances_vgbackup};
  my $instances = $$self{_instances_vgbackup} = {}; 
  my $vgbackup = $self->vgbackup;
  my $boot = $self->attachments_boot;
  my $blck = $self->attachments_blck;
  my $vgs = $self->vgs;
  my $name = $self->instances;
  $$instances{$_}{boot} = $$boot{$_} for keys %$boot;
  $$instances{$_}{blck} = [grep {defined $_ and length $_} split /\|/,$$blck{$_}] for keys %$blck;
  local $_ = [map {$_=[split /\|/,$_]} @$vgbackup];
  undef $vgbackup;
  foreach(@$_){
    my $vg_id = shift @$_;
    my %vgbackup; @vgbackup{@fields_vgbackup} = @$_;
    push @{$$vgbackup{$vg_id}},{%vgbackup}}
  undef $_;
  foreach my $instance_id (keys %$instances){
    $$instances{$instance_id}{boot} ||= '';
    $$instances{$instance_id}{blck} ||= [];
    foreach my $volume (grep {$_} $$instances{$instance_id}{boot},@{$$instances{$instance_id}{blck}}){
      my $vg_id = $$vgs{$volume} or next;
      push @{$$instances{$instance_id}{vgid}{$vg_id}{volumes}},$volume;
      $$instances{$instance_id}{vgid}{$vg_id}{backups} = $$vgbackup{$vg_id}}}
  $$instances{$_}{name} = $$name{$_} for keys %$name;
  $$self{_instances_vgbackup};
}
sub report01{
  my $self = shift;
  my $vgbackup = $self->instances_vgbackup;
  my $data;
  push @$data,['name',@fields_vgbackup];
  foreach my $instance_id (keys %$vgbackup){
    my $name = $$vgbackup{$instance_id}{name};
    my $vg = $$vgbackup{$instance_id}{vgid};
    if($vg && ref $vg eq 'HASH' && scalar keys %$vg){
      foreach my $vg_id (keys %$vg){
        my $backups = $$vg{$vg_id}{backups};
        if($backups && ref $backups eq 'ARRAY' && $#$backups>=0){
          push @$data,[$name,@$_{@fields_vgbackup}] for @$backups;
        }else{
          push @$data,[$name,map {''} @$_{@fields_vgbackup}];
        }
      }
    }else{
      push @$data,[$name,map {''} @$_{@fields_vgbackup}];
    }
  }
  print '"'.join('","',map {defined($_)?$_:''} @$_).'"'.$/ for @$data;
}
sub _attachments_params{
  my $self = shift;
  return $$self{_attachments_params} if $$self{_attachments_params};
  $$self{_attachments_params} = [];
  my $avdomains = $self->avdomains;
  my $compartments = $self->compartments;
  my @params;
  foreach my $avdomain (@$avdomains){
    foreach my $c (@$compartments){
      push @params, "--availability-domain $avdomain --compartment-id $c"}}
  $$self{_attachments_params} = [@params];
  $$self{_attachments_params};
}
sub _attachments_boot_fetch{
  my $at = shift;
  local $_ = qx/oci compute boot-volume-attachment list --all $at/;
  die "Cannot fetch boot attachments$/" if $?;
  exit unless $_ && length;
  my $json = decode_json $_;
  my %attachments_boot;
  $attachments_boot{$$_{'instance-id'}} = $$_{'boot-volume-id'} for @{$$json{data}};
  Storable::store \%attachments_boot, "__attachments_boot__$$";
}
sub attachments_boot{
  my $self = shift;
  return $$self{_attachments_boot} if $$self{_attachments_boot};
  my $params = $self->_attachments_params;
  warn localtime()." |  fetching attachment boot volumes$/";
  my $fc = Proc::ForkController->new;
  $fc->code('_attachments_boot_fetch');
  $fc->params(@$params);
  $fc->start;
  warn localtime()." |  fetching attachment boot volumes OK$/";
  my %attachments_boot;
  foreach my $file (glob '__attachments_boot__*'){
    my $ref = Storable::retrieve $file or die "Cannot retrieve file: $!$/";
    unlink $file or warn "Could not unlink $file: $!$/";
    $attachments_boot{$_} = $$ref{$_} for keys %$ref}
  $$self{_attachments_boot} = {%attachments_boot};
}
sub _attachments_blck_fetch{
  my $at = shift;
  local $_ = qx/oci compute volume-attachment list --all $at/;
  die "Cannot fetch blck attachments$/" if $?;
  exit unless $_ && length;
  my $json = decode_json $_;
  my %attachments_blck;
  $attachments_blck{$$_{'instance-id'}} .= $$_{'volume-id'}.'|' for @{$$json{data}};
  Storable::store \%attachments_blck, "__attachments_blck__$$";
}
sub attachments_blck{
  my $self = shift;
  return $$self{_attachments_blck} if $$self{_attachments_blck};
  my $params = $self->_attachments_params;
  warn localtime()." |  fetching attachment block volumes$/";
  my $fc = Proc::ForkController->new;
  $fc->code('_attachments_blck_fetch');
  $fc->params(@$params);
  $fc->start;
  warn localtime()." |  fetching attachment block volumes OK$/";
  my %attachments_blck;
  foreach my $file (glob '__attachments_blck__*'){
    my $ref = Storable::retrieve $file or die "Cannot retrieve file: $!$/";
    unlink $file or warn "Could not unlink $file: $!$/";
    $attachments_blck{$_} = $$ref{$_} for keys %$ref}
  $$self{_attachments_blck} = {%attachments_blck};
}
sub _instances_fetch{
  my $c = shift;
  local $_ = qx/oci compute instance list --all --compartment-id $c/;
  die "Cannot fetch instances$/" if $?;
  exit unless $_ && length;
  my $json = decode_json $_;
  my %instances;
  $instances{$$_{id}} = $$_{'display-name'} for @{$$json{data}};
  Storable::store \%instances, "__instances__$$";
}
sub instances{
  my $self = shift;
  return $$self{_instances} if $$self{_instances};
  $$self{_instances} = {};
  my $compartments = $self->compartments;
  warn localtime()." |  fetching instance names$/";
  my $fc = Proc::ForkController->new;
  $fc->code('_instances_fetch');
  $fc->params(@$compartments);
  $fc->start;
  warn localtime()." |  fetching instance names OK$/";
  my %instances;
  foreach my $file (glob '__instances__*'){
    my $ref = Storable::retrieve $file or die "Cannot retrieve file: $!$/";
    unlink $file or warn "Could not unlink $file: $!$/";
    $instances{$_} = $$ref{$_} for keys %$ref}
  $$self{_instances} = {%instances};
}
sub _vgs_fetch{
  my $c = shift;
  local $_ = qx/oci bv volume-group list --all --compartment-id $c/;
  die "Cannot fetch vgs$/" if $?;
  exit unless $_ && length;
  my $json = decode_json $_;
  my %vgs;
  foreach my $data (@{$$json{data}}){
    $vgs{$_} = $$data{id} for @{$$data{'volume-ids'}}}
  Storable::store \%vgs, "__vgs__$$";
}
sub vgs{
  my $self = shift;
  return $$self{_vgs} if $$self{_vgs};
  $$self{_vgs} = {};
  my $compartments = $self->compartments;
  warn localtime()." |  fetching volume groups$/";
  my $fc = Proc::ForkController->new;
  $fc->code('_vgs_fetch');
  $fc->params(@$compartments);
  $fc->start;
  warn localtime()." |  fetching volume groups OK$/";
  my %vgs;
  foreach my $file (glob '__vgs__*'){
    my $ref = Storable::retrieve $file or die "Cannot retrieve file: $!$/";
    unlink $file or warn "Could not unlink $file: $!$/";
    $vgs{$_} = $$ref{$_} for keys %$ref}
  $$self{_vgs} = {%vgs};
}
sub _vgbackup_fetch{
  my $c = shift;
  local $_ = qx/oci bv volume-group-backup list --all --compartment-id $c/;
  die "Cannot fetch vgbackup$/" if $?;
  exit unless $_ && length;
  my $json = decode_json $_;
  my @vgbackup;
  push @vgbackup, join '|', map {defined $_ ? $_ : ''} @$_{'volume-group-id',@fields_vgbackup} for @{$$json{data}};
  Storable::store \@vgbackup, "__vgbackup__$$";
}
sub vgbackup{
  my $self = shift;
  $$self{_vgbackup} = [];
  my $compartments = $self->compartments;
  warn localtime()." |  fetching volume group backups$/";
  my $fc = Proc::ForkController->new;
  $fc->code('_vgbackup_fetch');
  $fc->params(@$compartments);
  $fc->start;
  warn localtime()." |  fetching volume group backups OK$/";
  my @vgbackup;
  foreach my $file (glob '__vgbackup__*'){
    my $ref = Storable::retrieve $file or die "Cannot retrieve file: $!$/";
    unlink $file or warn "Could not unlink $file: $!$/";
    push @vgbackup,@$ref}
  $$self{_vgbackup} = [@vgbackup];
}
1;
