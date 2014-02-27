<?php
interface ITripodStat
{
    public function increment($operation);
    public function timer($operation,$duration);
}