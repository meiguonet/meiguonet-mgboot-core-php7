<?php

namespace mgboot\task;

interface CronTask
{
    public function run(): void;
}
