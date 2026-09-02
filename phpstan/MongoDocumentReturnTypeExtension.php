<?php

namespace Tripod\PHPStan;

use MongoDB\Collection;
use PhpParser\Node\Expr\MethodCall;
use PHPStan\Analyser\Scope;
use PHPStan\Reflection\MethodReflection;
use PHPStan\Type\ArrayType;
use PHPStan\Type\DynamicMethodReturnTypeExtension;
use PHPStan\Type\MixedType;
use PHPStan\Type\StringType;
use PHPStan\Type\Type;
use PHPStan\Type\TypeCombinator;

/**
 * Narrows the array|object|null return type of MongoDB\Collection::findOne()
 * and friends to array<string, mixed>|null. Tripod always constructs
 * MongoDB\Client with a typeMap of root/document/array => 'array'
 * (see \Tripod\Mongo\Config::getMongoClient()), so documents returned by the
 * driver are always plain PHP arrays, never BSON objects. A stub file cannot
 * express this because PHPStan keeps unmatched members of a native union type.
 */
class MongoDocumentReturnTypeExtension implements DynamicMethodReturnTypeExtension
{
    private const METHODS = ['findOne', 'findOneAndDelete', 'findOneAndReplace', 'findOneAndUpdate'];

    public function getClass(): string
    {
        return Collection::class;
    }

    public function isMethodSupported(MethodReflection $methodReflection): bool
    {
        return in_array($methodReflection->getName(), self::METHODS, true);
    }

    public function getTypeFromMethodCall(MethodReflection $methodReflection, MethodCall $methodCall, Scope $scope): Type
    {
        return TypeCombinator::addNull(new ArrayType(new StringType(), new MixedType()));
    }
}
